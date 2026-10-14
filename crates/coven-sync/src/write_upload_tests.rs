use super::*;
use async_trait::async_trait;
use coven_storage::{
    AccessGrant, ByteRange, RetainedAccess, StorageConfig, StorageError, StorageFailure,
    StoredObject, UploadSession,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct BudgetStorage {
    provider: Arc<MemoryStorage>,
    budget: usize,
    largest: AtomicUsize,
    conflict_on_finish: bool,
    cleanup_failure: std::sync::Mutex<Option<UploadCall>>,
}
#[derive(Clone, Copy, PartialEq)]
enum UploadCall {
    Begin,
    Resume,
    Finish,
}
impl BudgetStorage {
    fn check_cleanup(&self, call: UploadCall) -> Result<(), StorageError> {
        if *self.cleanup_failure.lock().unwrap() == Some(call) {
            return Err(StorageError::Cleanup {
                operation: Box::new(coven_storage::StorageFailure::AlreadyExists.into()),
                cleanup: Box::new(coven_storage::StorageFailure::PermissionDenied.into()),
            });
        }
        Ok(())
    }
    fn check(&self, length: usize) {
        assert!(
            length <= self.budget,
            "transfer buffer {length} exceeds {}",
            self.budget
        );
        self.largest.fetch_max(length, Ordering::SeqCst);
    }
}
struct BudgetBody<'a> {
    owner: &'a BudgetStorage,
    stream: coven_storage::ObjectStream<'a>,
}
#[async_trait]
impl coven_storage::ObjectBody for BudgetBody<'_> {
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        self.owner.check(output.len());
        self.stream.read(output).await
    }
}
#[async_trait]
impl Storage for BudgetStorage {
    fn config(&self) -> StorageConfig {
        self.provider.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.provider.single_request_limit()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.check(bytes.len());
        self.provider.create(path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.check(bytes.len());
        self.provider.replace(path, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.provider.status(path).await
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<coven_storage::ObjectStream<'_>, StorageError> {
        let stream = self.provider.stream_object(path, range).await?;
        let metadata = stream.metadata().clone();
        coven_storage::ObjectStream::new(
            metadata,
            range,
            BudgetBody {
                owner: self,
                stream,
            },
        )
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.provider.list(prefix).await
    }
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        self.provider.list_writers(store).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.provider.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.provider.grant_access(account).await
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.provider.revoke_access(account).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.check_cleanup(UploadCall::Begin)?;
        self.provider.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        if self.conflict_on_finish {
            return Err(conflict(session));
        }
        self.check_cleanup(UploadCall::Resume)?;
        self.provider.resume_upload(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.check(bytes.len());
        self.provider.upload_part(session, bytes).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        if self.conflict_on_finish {
            return Err(conflict(session));
        }
        self.check_cleanup(UploadCall::Finish)?;
        self.provider.finish_upload(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.provider.abort_upload(session).await
    }
}

#[tokio::test]
async fn a_write_exceeds_the_transfer_budget_on_upload_and_download() {
    const BUDGET: usize = 64 * 1024 + 44;
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let bounded = Arc::new(BudgetStorage {
        provider: storage.clone(),
        budget: BUDGET,
        largest: AtomicUsize::new(0),
        conflict_on_finish: false,
        cleanup_failure: std::sync::Mutex::new(None),
    });
    for device in &mut devices {
        device.writes = DeviceLogSync::new(
            bounded.clone(),
            device.db.clone(),
            device.custody.clone(),
            device.identity.clone(),
            device.clock.clone(),
            device.send.clone(),
        );
    }
    devices[0]
        .db
        .write(|sql| {
            let body = "x".repeat(16 * 1024);
            for i in 0..512 {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,'title',?2)",
                    (i.to_string(), &body),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(devices[0].writes.upload_writes().await.unwrap().len(), 1);
    let objects = storage
        .list(&ObjectPrefix::device_logs(STORE_ID))
        .await
        .unwrap();
    assert!(objects[0].size > 100 * BUDGET as u64);
    devices[1].writes.download_writes().await.unwrap();
    assert!(bounded.largest.load(Ordering::SeqCst) >= 64 * 1024);
    assert_eq!(
        devices[1]
            .db
            .read(|sql| Ok(sql.query_row(
                "SELECT count(*),sum(length(body)) FROM notes",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            )?))
            .await
            .unwrap(),
        (512, 512 * 16 * 1024)
    );
}

// Inject the provider's failed-call state change; the real upload consumers
// must record it before returning either failure to their initiator.
fn conflict(session: &mut UploadSession) -> StorageError {
    let mut recording = serde_json::to_value(&*session).unwrap();
    recording["confirmed"] = serde_json::json!(0);
    recording["part_size"] = serde_json::json!(8 * 1024 * 1024);
    recording["state"] = serde_json::json!({"S3Restart": {"id": "upload", "token": "token"}});
    *session = UploadSession::decode(&serde_json::to_vec(&recording).unwrap()).unwrap();
    StorageError::Cleanup {
        operation: Box::new(StorageFailure::SessionExpired.into()),
        cleanup: Box::new(StorageFailure::PermissionDenied.into()),
    }
}

fn assert_restart_recorded(session: &UploadSession) {
    assert_eq!(session.confirmed_bytes(), 0);
    let value = serde_json::to_value(session).unwrap();
    assert!(value["state"].get("S3Restart").is_some());
}

#[tokio::test]
async fn write_publication_records_invalidated_session_before_returning_abort_failure() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    devices[0].writes = DeviceLogSync::new(
        Arc::new(BudgetStorage {
            provider: storage,
            budget: 64 * 1024 + 44,
            largest: AtomicUsize::new(0),
            conflict_on_finish: true,
            cleanup_failure: std::sync::Mutex::new(None),
        }),
        devices[0].db.clone(),
        devices[0].custody.clone(),
        devices[0].identity.clone(),
        devices[0].clock.clone(),
        devices[0].send.clone(),
    );
    devices[0]
        .db
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('one','title',?1)",
                ["x".repeat(100_000)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let write = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    for _ in 0..2 {
        assert!(matches!(
            devices[0].writes.upload_writes().await,
            Err(SyncError::Storage(StorageError::Cleanup { .. }))
        ));
        let recording = devices[0]
            .db
            .write_upload_session(write)
            .await
            .unwrap()
            .unwrap();
        assert_restart_recorded(&UploadSession::decode(&recording).unwrap());
    }
}

#[tokio::test]
async fn recorded_publication_saves_invalidated_session_on_finish_and_resume_failure() {
    use crate::recorded_upload::{upload, UploadSource};
    struct Source {
        saved: Option<UploadSession>,
        fail_save: bool,
    }
    impl UploadSource for Source {
        type Error = StorageError;
        async fn read(&mut self, _offset: u64, length: usize) -> Result<Vec<u8>, Self::Error> {
            Ok(vec![7; length])
        }
        async fn save(&mut self, session: UploadSession) -> Result<(), Self::Error> {
            if self.fail_save {
                return Err(StorageFailure::File.into());
            }
            self.saved = Some(session);
            Ok(())
        }
        fn keep_going(&self) -> bool {
            true
        }
    }
    let storage = BudgetStorage {
        provider: storage(),
        budget: 64 * 1024 + 44,
        largest: AtomicUsize::new(0),
        conflict_on_finish: true,
        cleanup_failure: std::sync::Mutex::new(None),
    };
    let path = ObjectPath::device_log(STORE_ID, DeviceId(1), std::num::NonZeroU64::MIN);
    let mut source = Source {
        saved: None,
        fail_save: false,
    };
    for _ in 0..2 {
        let recorded = source.saved.take();
        let error = upload(&storage, &path, 4, recorded, &mut source)
            .await
            .unwrap_err();
        assert!(matches!(error, StorageError::Cleanup { operation, cleanup }
            if operation.failure() == StorageFailure::SessionExpired && cleanup.failure() == StorageFailure::PermissionDenied));
        assert_restart_recorded(source.saved.as_ref().unwrap());
    }
    source.fail_save = true;
    let recorded = source.saved.take();
    let error = upload(&storage, &path, 4, recorded, &mut source)
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::File);
    let error = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<StorageError>()
        .unwrap();
    assert!(matches!(error, StorageError::Cleanup { operation, cleanup }
        if matches!(**operation, StorageError::Cleanup { .. }) && cleanup.failure() == StorageFailure::File));
}

#[tokio::test]
async fn occupied_publication_with_failed_cleanup_keeps_the_write_pending() {
    for call in [UploadCall::Begin, UploadCall::Resume, UploadCall::Finish] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        devices[0]
            .db
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('one','title',?1)",
                    ["x".repeat(100_000)],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        if call == UploadCall::Resume {
            let mut faults = Faults::none();
            faults.lose_part_reply = true;
            storage.set_faults(faults).await;
            assert!(devices[0].writes.upload_writes().await.is_err());
            storage.set_faults(Faults::none()).await;
        }
        let faulty = Arc::new(BudgetStorage {
            provider: storage.clone(),
            budget: 64 * 1024 + 44,
            largest: AtomicUsize::new(0),
            cleanup_failure: std::sync::Mutex::new(Some(call)),
            conflict_on_finish: false,
        });
        devices[0].writes = DeviceLogSync::new(
            faulty.clone(),
            devices[0].db.clone(),
            devices[0].custody.clone(),
            devices[0].identity.clone(),
            devices[0].clock.clone(),
            devices[0].send.clone(),
        );
        assert!(matches!(
            devices[0].writes.upload_writes().await,
            Err(SyncError::Storage(StorageError::Cleanup { .. }))
        ));
        let write = WriteId {
            device: DeviceId(1),
            number: 1,
        };
        if call != UploadCall::Begin {
            assert!(devices[0]
                .db
                .write_upload_session(write)
                .await
                .unwrap()
                .is_some());
        }
        *faulty.cleanup_failure.lock().unwrap() = None;
        assert_eq!(devices[0].writes.upload_writes().await.unwrap(), [write]);
    }
}

#[tokio::test]
async fn lost_part_replies_and_expired_sessions_preserve_order_and_bytes() {
    for expire in [false, true] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        devices[0]
            .db
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('one','first',?1)",
                    ["x".repeat(200_000)],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('two','second','body')",
        )
        .await;
        let mut faults = Faults::none();
        faults.lose_part_reply = true;
        storage.set_faults(faults).await;
        assert!(
            matches!(devices[0].writes.upload_writes().await, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network)
        );
        assert!(storage
            .list(&ObjectPrefix::device_logs(STORE_ID))
            .await
            .unwrap()
            .is_empty());
        let fixed = writes::resealed(&devices[0]).await.1;
        let mut faults = Faults::none();
        faults.expire_uploads = expire;
        storage.set_faults(faults).await;
        assert_eq!(
            devices[0].writes.upload_writes().await.unwrap(),
            [
                WriteId {
                    device: DeviceId(1),
                    number: 1
                },
                WriteId {
                    device: DeviceId(1),
                    number: 2
                }
            ]
        );
        assert_eq!(
            storage
                .read(&ObjectPath::device_log(
                    STORE_ID,
                    DeviceId(1),
                    1.try_into().unwrap()
                ))
                .await
                .unwrap(),
            fixed
        );
        devices[1].writes.download_writes().await.unwrap();
        assert_eq!(rows(&devices[1].db).await, rows(&devices[0].db).await);
    }
}

#[tokio::test]
async fn classified_occupied_paths_count_as_stored_with_and_without_a_session() {
    for resume in [false, true] {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        devices[0]
            .db
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('one','title',?1)",
                    ["x".repeat(100_000)],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let mut fault = Faults::none();
        if resume {
            fault.lose_part_reply = true;
        } else {
            fault.fail_next = 1;
        }
        storage.set_faults(fault).await;
        assert!(
            matches!(devices[0].writes.upload_writes().await, Err(SyncError::Storage(error)) if error.failure() == coven_storage::StorageFailure::Network)
        );
        let fixed = writes::resealed(&devices[0]).await;
        storage
            .create(&crate::write_seal::path(STORE_ID, fixed.0), &fixed.1)
            .await
            .unwrap();
        let mut fault = Faults::none();
        fault.fail_next = 1;
        fault.failure = coven_storage::StorageFailure::AlreadyExists;
        storage.set_faults(fault).await;
        assert_eq!(devices[0].writes.upload_writes().await.unwrap(), [fixed.0]);
    }
}

#[tokio::test]
async fn a_completed_catch_up_expires_at_five_minutes_before_an_outside_write_send() {
    for (age, allowed) in [
        (Duration::from_millis(299_999), true),
        (Duration::from_secs(300), false),
    ] {
        let storage = storage();
        let mut devices = group(storage.clone(), 1).await;
        let sender = &mut devices[0];
        sql(
            &sender.db,
            "INSERT INTO notes VALUES('expiry','title','body')",
        )
        .await;
        let now = coven_foundation::clock::Clock::now(sender.clock.as_ref());
        sender.clock.set(now + age);
        let before = storage.request_trace().len();
        let result = sender.writes.upload_writes().await;
        assert_eq!(result.is_ok(), allowed, "age {age:?}: {result:?}");
        let sent = storage.request_trace()[before..].iter().any(|request| {
            matches!(
                request, coven_storage::test_utils::MemoryRequest::Create(path)
                    if path.write_position().is_some()
            )
        });
        assert_eq!(sent, allowed);
        assert_eq!(
            sender.db.test_queued_writes().await.unwrap().len(),
            usize::from(!allowed)
        );
    }
}
