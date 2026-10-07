use super::*;
use async_trait::async_trait;
use coven_storage::{
    AccessGrant, ByteRange, MemberAccess, MemberRemoval, StorageError, StoredObject, UploadSession,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct BudgetStorage {
    provider: Arc<MemoryStorage>,
    budget: usize,
    largest: AtomicUsize,
}
impl BudgetStorage {
    fn check(&self, length: usize) {
        assert!(
            length <= self.budget,
            "transfer buffer {length} exceeds {}",
            self.budget
        );
        self.largest.fetch_max(length, Ordering::SeqCst);
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
    async fn read(&self, _: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        panic!("write download must use bounded ranges")
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.check(range.len() as usize);
        self.provider.read_range(path, range).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.provider.list(prefix).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.provider.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.provider.grant_access(account).await
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        self.provider.revoke_access(member).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.provider.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
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
    });
    for device in &mut devices {
        device.sync = DeviceLogSync::new(
            bounded.clone(),
            device.db.clone(),
            device.keys.clone(),
            device.identity.clone(),
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
    assert_eq!(devices[0].sync.upload_writes().await.unwrap().len(), 1);
    let objects = storage.list(&ObjectPrefix::device_logs()).await.unwrap();
    assert!(objects[0].size > 100 * BUDGET as u64);
    let report = devices[1].sync.download_writes().await.unwrap();
    assert!(report.waiting.is_empty(), "{report:?}");
    assert!(report.damaged_objects.is_empty(), "{report:?}");
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
        assert!(devices[0].sync.upload_writes().await.is_err());
        assert!(storage
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .is_empty());
        let fixed = writes::resealed(&devices[0]).await.1;
        let mut faults = Faults::none();
        faults.expire_uploads = expire;
        storage.set_faults(faults).await;
        assert_eq!(
            devices[0].sync.upload_writes().await.unwrap(),
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
                .read(&ObjectPath::device_log(DeviceId(1), 1.try_into().unwrap()))
                .await
                .unwrap(),
            fixed
        );
        let report = devices[1].sync.download_writes().await.unwrap();
        assert!(report.damaged_objects.is_empty(), "{report:?}");
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
        assert!(devices[0].sync.upload_writes().await.is_err());
        let fixed = writes::resealed(&devices[0]).await;
        storage
            .create(&crate::write_seal::path(fixed.0), &fixed.1)
            .await
            .unwrap();
        let mut fault = Faults::none();
        fault.fail_next = 1;
        fault.failure = coven_storage::StorageFailure::AlreadyExists;
        storage.set_faults(fault).await;
        assert_eq!(devices[0].sync.upload_writes().await.unwrap(), [fixed.0]);
    }
}
