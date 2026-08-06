use super::chunking::*;
use super::exact::*;
use super::*;
use crate::cloud::{no_progress, BlobBody};
use coven_foundation::id_provider::SequentialIdProvider;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

fn exact_slot(key: &str) -> ObjectSlot {
    ObjectSlot::logical(key.to_string()).expect("valid exact slot")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MockCall {
    Write(String),
    Read(String),
    List(String),
    Delete(String),
    Exists(String),
    BeginBatch(String),
    Stage(String),
    CommitBatch(String),
    DiscardBatch(String),
    DeleteVersions(Vec<String>),
}

struct PausedWrite {
    key: String,
    stored: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

struct MockCloudKitOps {
    store: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
    versions: Mutex<HashMap<(CloudKitScope, String), u64>>,
    calls: Mutex<Vec<MockCall>>,
    fail_deletes: Mutex<HashSet<String>>,
    fail_delete_once: Mutex<HashMap<String, usize>>,
    fail_writes: Mutex<HashSet<String>>,
    staged_batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
    next_batch: AtomicUsize,
    max_stage_payload: AtomicUsize,
    fail_discards: AtomicBool,
    lose_commit_response: AtomicBool,
    return_wrong_commit_keys: AtomicBool,
    pause_write_after_store: Mutex<Option<PausedWrite>>,
    record_exists_calls: AtomicUsize,
    /// Every versioned-record fetch, by key. Kept apart from `calls` so a
    /// test can count which records a read touched without disturbing the
    /// call-sequence assertions the ledger already carries.
    versioned_reads: Mutex<Vec<String>>,
    grant_share_calls: AtomicUsize,
    revoke_share_calls: AtomicUsize,
    shares: Mutex<HashMap<String, CloudKitShare>>,
}

impl MockCloudKitOps {
    fn versioned_reads(&self) -> Vec<String> {
        self.versioned_reads.lock().unwrap().clone()
    }

    fn clear_versioned_reads(&self) {
        self.versioned_reads.lock().unwrap().clear();
    }

    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            versions: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
            fail_deletes: Mutex::new(HashSet::new()),
            fail_delete_once: Mutex::new(HashMap::new()),
            fail_writes: Mutex::new(HashSet::new()),
            staged_batches: Mutex::new(HashMap::new()),
            next_batch: AtomicUsize::new(0),
            max_stage_payload: AtomicUsize::new(0),
            fail_discards: AtomicBool::new(false),
            lose_commit_response: AtomicBool::new(false),
            return_wrong_commit_keys: AtomicBool::new(false),
            pause_write_after_store: Mutex::new(None),
            record_exists_calls: AtomicUsize::new(0),
            versioned_reads: Mutex::new(Vec::new()),
            grant_share_calls: AtomicUsize::new(0),
            revoke_share_calls: AtomicUsize::new(0),
            shares: Mutex::new(HashMap::new()),
        }
    }

    fn calls(&self) -> Vec<MockCall> {
        self.calls.lock().unwrap().clone()
    }

    fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    fn fail_delete(&self, key: &str) {
        self.fail_deletes.lock().unwrap().insert(key.to_string());
    }

    fn fail_next_delete(&self, key: &str) {
        self.fail_delete_once
            .lock()
            .unwrap()
            .insert(key.to_string(), 1);
    }

    fn fail_write(&self, key: &str) {
        self.fail_writes.lock().unwrap().insert(key.to_string());
    }

    fn fail_discard(&self) {
        self.fail_discards.store(true, Ordering::SeqCst);
    }

    fn lose_commit_response(&self) {
        self.lose_commit_response.store(true, Ordering::SeqCst);
    }

    fn return_wrong_commit_keys(&self) {
        self.return_wrong_commit_keys.store(true, Ordering::SeqCst);
    }

    fn pause_write_after_store(
        &self,
        key: &str,
    ) -> (Arc<std::sync::Barrier>, Arc<std::sync::Barrier>) {
        let stored = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let previous = self
            .pause_write_after_store
            .lock()
            .unwrap()
            .replace(PausedWrite {
                key: key.to_string(),
                stored: stored.clone(),
                release: release.clone(),
            });
        assert!(previous.is_none(), "a CloudKit write is already paused");
        (stored, release)
    }

    fn write_chunk_manifest(&self, key: &str, total_len: usize) {
        self.write_record(
            &CloudKitScope::Private,
            &chunk_manifest_key(key),
            encode_chunk_manifest(ChunkManifest::new(
                total_len,
                "0123456789abcdef0123456789abcdef".to_string(),
            )),
        )
        .unwrap();
    }

    fn write_chunk_part(&self, key: &str, index: usize, data: Vec<u8>) {
        self.write_record(
            &CloudKitScope::Private,
            &chunk_part_key(key, "0123456789abcdef0123456789abcdef", index),
            data,
        )
        .unwrap();
    }
}

impl CloudKitOps for MockCloudKitOps {
    fn provider_identity(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitProviderIdentity, CloudHomeError> {
        let (owner_name, zone_name) = match scope {
            CloudKitScope::Private => ("private-owner", "private-zone"),
            CloudKitScope::Shared {
                owner_name,
                zone_name,
            } => (owner_name.as_str(), zone_name.as_str()),
        };
        Ok(CloudKitProviderIdentity {
            container_id: "iCloud.example.coven".to_string(),
            environment: coven_protocol::objects::CloudKitEnvironment::Development,
            owner_name: owner_name.to_string(),
            zone_name: zone_name.to_string(),
            current_user_record_name: "current-user".to_string(),
        })
    }

    fn accepted_read_write_share(
        &self,
        scope: &CloudKitScope,
    ) -> Result<CloudKitAcceptedShareRecord, CloudHomeError> {
        let CloudKitScope::Shared {
            owner_name,
            zone_name,
        } = scope
        else {
            return Err(CloudHomeError::NotFound(
                "accepted CloudKit share".to_string(),
            ));
        };
        Ok(CloudKitAcceptedShareRecord {
            share_record_name: "accepted-share".to_string(),
            owner_name: owner_name.clone(),
            zone_name: zone_name.clone(),
            participant_record_name: "current-user".to_string(),
            permission: CloudKitSharePermission::ReadWrite,
            acceptance: CloudKitShareAcceptance::Accepted,
            canonical_record: b"canonical accepted CKShare".to_vec(),
        })
    }

    fn write_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::Write(key.to_string()));
        if self.fail_writes.lock().unwrap().contains(key) {
            return Err(CloudHomeError::Transport(format!("write {key} failed")));
        }
        let record = (scope.clone(), key.to_string());
        self.store.lock().unwrap().insert(record.clone(), data);
        let mut versions = self.versions.lock().unwrap();
        let next = versions.get(&record).copied().unwrap_or(0) + 1;
        versions.insert(record, next);
        drop(versions);
        let pause = {
            let mut pause = self.pause_write_after_store.lock().unwrap();
            match pause.as_ref() {
                Some(paused) if paused.key == key => pause.take(),
                _ => None,
            }
        };
        if let Some(paused) = pause {
            assert_eq!(paused.key, key);
            paused.stored.wait();
            paused.release.wait();
        }
        Ok(())
    }

    fn read_record(&self, scope: &CloudKitScope, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::Read(key.to_string()));
        self.store
            .lock()
            .unwrap()
            .get(&(scope.clone(), key.to_string()))
            .cloned()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
    }

    fn list_records(
        &self,
        scope: &CloudKitScope,
        prefix: &str,
    ) -> Result<Vec<String>, CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::List(prefix.to_string()));
        let store = self.store.lock().unwrap();
        let mut keys: Vec<String> = store
            .keys()
            .filter(|(record_scope, key)| record_scope == scope && key.starts_with(prefix))
            .map(|(_, key)| key.clone())
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn delete_record(&self, scope: &CloudKitScope, key: &str) -> Result<(), CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::Delete(key.to_string()));
        if self.fail_deletes.lock().unwrap().contains(key) {
            return Err(CloudHomeError::Transport(format!("delete {key} failed")));
        }
        if let Some(remaining) = self.fail_delete_once.lock().unwrap().get_mut(key) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(CloudHomeError::Transport(format!("delete {key} failed")));
            }
        }
        self.store
            .lock()
            .unwrap()
            .remove(&(scope.clone(), key.to_string()));
        self.versions
            .lock()
            .unwrap()
            .remove(&(scope.clone(), key.to_string()));
        Ok(())
    }

    fn record_exists(&self, scope: &CloudKitScope, key: &str) -> Result<bool, CloudHomeError> {
        self.record_exists_calls.fetch_add(1, Ordering::Relaxed);
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::Exists(key.to_string()));
        Ok(self
            .store
            .lock()
            .unwrap()
            .contains_key(&(scope.clone(), key.to_string())))
    }

    fn read_versioned_record(
        &self,
        scope: &CloudKitScope,
        key: &str,
    ) -> Result<CloudVersionedObject, CloudHomeError> {
        self.versioned_reads.lock().unwrap().push(key.to_string());
        let record = (scope.clone(), key.to_string());
        let store = self.store.lock().unwrap();
        let versions = self.versions.lock().unwrap();
        Ok(CloudVersionedObject {
            bytes: store
                .get(&record)
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?,
            version: CloudObjectVersion::from_provider(
                versions
                    .get(&record)
                    .copied()
                    .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?
                    .to_string(),
            )?,
        })
    }

    fn begin_atomic_create(
        &self,
        _scope: &CloudKitScope,
    ) -> Result<CloudKitAtomicCreateBatch, CloudHomeError> {
        let batch = CloudKitAtomicCreateBatch::from_provider(format!(
            "batch-{}",
            self.next_batch.fetch_add(1, Ordering::SeqCst)
        ))?;
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::BeginBatch(batch.as_provider().to_string()));
        self.staged_batches
            .lock()
            .unwrap()
            .insert(batch.as_provider().to_string(), Vec::new());
        Ok(batch)
    }

    fn stage_atomic_create_record(
        &self,
        _scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
        record: CloudKitRecordCreate,
    ) -> Result<(), CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::Stage(record.key.clone()));
        self.max_stage_payload
            .fetch_max(record.data.len(), Ordering::SeqCst);
        self.staged_batches
            .lock()
            .unwrap()
            .get_mut(batch.as_provider())
            .ok_or_else(|| {
                CloudHomeError::NotFound(format!(
                    "CloudKit staging batch {:?}",
                    batch.as_provider()
                ))
            })?
            .push(record);
        Ok(())
    }

    fn commit_atomic_create(
        &self,
        scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<Vec<CloudKitRecordVersion>, CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::CommitBatch(batch.as_provider().to_string()));
        let mut batches = self.staged_batches.lock().unwrap();
        let records = batches.get(batch.as_provider()).ok_or_else(|| {
            CloudHomeError::NotFound(format!("CloudKit staging batch {:?}", batch.as_provider()))
        })?;
        let fail_writes = self.fail_writes.lock().unwrap();
        let mut store = self.store.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        for record in records {
            if fail_writes.contains(&record.key) {
                return Err(CloudHomeError::Transport(format!(
                    "atomic create {:?} failed",
                    record.key
                )));
            }
            if store.contains_key(&(scope.clone(), record.key.clone())) {
                return Err(CloudHomeError::AlreadyExists(record.key.clone()));
            }
        }
        let records = batches
            .remove(batch.as_provider())
            .expect("validated CloudKit staging batch disappeared");
        let mut created = Vec::with_capacity(records.len());
        for record in records {
            let coordinate = (scope.clone(), record.key.clone());
            store.insert(coordinate.clone(), record.data);
            versions.insert(coordinate, 1);
            created.push(CloudKitRecordVersion {
                key: record.key,
                version: CloudObjectVersion::from_provider("1".to_string())?,
            });
        }
        if self.lose_commit_response.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(
                "CloudKit commit response was lost".to_string(),
            ));
        }
        if self.return_wrong_commit_keys.load(Ordering::SeqCst) {
            for (index, record) in created.iter_mut().enumerate() {
                record.key = format!("unexpected-returned-record-{index}");
            }
        }
        Ok(created)
    }

    fn discard_atomic_create(
        &self,
        _scope: &CloudKitScope,
        batch: &CloudKitAtomicCreateBatch,
    ) -> Result<(), CloudHomeError> {
        self.calls
            .lock()
            .unwrap()
            .push(MockCall::DiscardBatch(batch.as_provider().to_string()));
        if self.fail_discards.load(Ordering::SeqCst) {
            return Err(CloudHomeError::Transport(format!(
                "discard staging batch {:?} failed",
                batch.as_provider()
            )));
        }
        self.staged_batches
            .lock()
            .unwrap()
            .remove(batch.as_provider());
        Ok(())
    }

    fn delete_record_versions(
        &self,
        scope: &CloudKitScope,
        records: &[CloudKitRecordVersion],
    ) -> Result<(), CloudHomeError> {
        self.calls.lock().unwrap().push(MockCall::DeleteVersions(
            records.iter().map(|record| record.key.clone()).collect(),
        ));
        let fail_deletes = self.fail_deletes.lock().unwrap();
        let mut store = self.store.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        for record in records {
            if fail_deletes.contains(&record.key) {
                return Err(CloudHomeError::Transport(format!(
                    "delete {:?} failed",
                    record.key
                )));
            }
            let storage_key = (scope.clone(), record.key.clone());
            let current = versions
                .get(&storage_key)
                .ok_or_else(|| CloudHomeError::NotFound(record.key.clone()))?;
            if current.to_string() != record.version.as_provider() {
                return Err(CloudHomeError::Transport(format!(
                    "CloudKit record {:?} changed before exact deletion",
                    record.key
                )));
            }
            if !store.contains_key(&storage_key) {
                return Err(CloudHomeError::NotFound(record.key.clone()));
            }
        }
        for record in records {
            let storage_key = (scope.clone(), record.key.clone());
            store.remove(&storage_key);
            versions.remove(&storage_key);
        }
        Ok(())
    }

    fn grant_share(&self, member_pubkey: &str) -> Result<CloudKitShare, CloudHomeError> {
        self.grant_share_calls.fetch_add(1, Ordering::Relaxed);
        let share = CloudKitShare {
            share_url: format!("https://share.example/{member_pubkey}"),
            owner_name: "owner-name".to_string(),
            zone_name: "bae-store".to_string(),
        };
        self.shares
            .lock()
            .unwrap()
            .insert(member_pubkey.to_string(), share.clone());
        Ok(share)
    }

    fn share_for_member(
        &self,
        member_pubkey: &str,
    ) -> Result<Option<CloudKitShare>, CloudHomeError> {
        Ok(self.shares.lock().unwrap().get(member_pubkey).cloned())
    }

    fn revoke_share(&self, member_pubkey: &str) -> Result<(), CloudHomeError> {
        self.revoke_share_calls.fetch_add(1, Ordering::Relaxed);
        self.shares.lock().unwrap().remove(member_pubkey);
        Ok(())
    }

    fn accept_share(&self, share_url: &str) -> Result<CloudKitShare, CloudHomeError> {
        Ok(CloudKitShare {
            share_url: share_url.to_string(),
            owner_name: "owner-name".to_string(),
            zone_name: "bae-store".to_string(),
        })
    }
}

fn make_cloud_home() -> CloudKitCloudHome {
    CloudKitCloudHome::new_private_with_ids(
        Arc::new(MockCloudKitOps::new()),
        Arc::new(SequentialIdProvider::new("cloudkit-upload")),
    )
}

fn make_cloud_home_with_ops() -> (CloudKitCloudHome, Arc<MockCloudKitOps>) {
    let ops = Arc::new(MockCloudKitOps::new());
    (
        CloudKitCloudHome::new_private_with_ids(
            ops.clone(),
            Arc::new(SequentialIdProvider::new("cloudkit-upload")),
        ),
        ops,
    )
}

#[tokio::test]
async fn provider_binding_uses_the_bridge_container_zone_and_current_user() {
    use coven_protocol::objects::{ProviderPrincipalId, StoreProviderBinding};
    let (home, _) = make_cloud_home_with_ops();

    let binding = ExactSlotStorage::provider_binding(&home)
        .await
        .expect("resolve CloudKit provider binding");

    assert_eq!(
        binding.store,
        StoreProviderBinding::CloudKit {
            container_id: "iCloud.example.coven".to_string(),
            environment: coven_protocol::objects::CloudKitEnvironment::Development,
            owner_name: "private-owner".to_string(),
            zone_name: "private-zone".to_string(),
        }
    );
    assert_eq!(
        binding.device.principal,
        ProviderPrincipalId::CloudKitPrivateZoneOwner {
            record_name: "current-user".to_string(),
        }
    );
}

struct FailingBodyReader {
    emitted: bool,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for FailingBodyReader {
    type Error = crate::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::local_file::PlaintextChunkError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(vec![7; CHUNK_SIZE]);
        }
        Err(crate::local_file::PlaintextChunkError::Local(
            "injected body failure".to_string(),
        ))
    }
}

struct PausedBodyReader {
    emitted: bool,
    waiting: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for PausedBodyReader {
    type Error = crate::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::local_file::PlaintextChunkError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(vec![7; CHUNK_SIZE]);
        }
        self.waiting.notify_one();
        self.release.notified().await;
        Ok(vec![8])
    }
}

#[tokio::test]
async fn mutable_body_failure_reports_cleanup_failure_and_drop_retries_cleanup() {
    let (home, ops) = make_cloud_home_with_ops();
    let part_key = chunk_part_key("mutable/body-failure", "cloudkit-upload-0", 0);
    ops.fail_next_delete(&part_key);
    let reader =
        crate::local_file::PlaintextReader::from_test_reader(FailingBodyReader { emitted: false });
    let body = BlobBody::from_test_reader((CHUNK_SIZE + 1) as u64, reader);

    let error = home
        .write("mutable/body-failure", body, &no_progress())
        .await
        .expect_err("body failure must report failed cleanup");

    assert!(
        matches!(error, CloudHomeError::CleanupFailed { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("injected body failure"),
        "{error}"
    );
    assert!(error.to_string().contains("delete"), "{error}");
    assert!(!ops
        .record_exists(&CloudKitScope::Private, &part_key)
        .expect("inspect canceled part"));
}

#[tokio::test]
async fn canceling_mutable_write_removes_every_staged_part() {
    let (home, ops) = make_cloud_home_with_ops();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let reader = crate::local_file::PlaintextReader::from_test_reader(PausedBodyReader {
        emitted: false,
        waiting: waiting.clone(),
        release: release.clone(),
    });
    let body = BlobBody::from_test_reader((CHUNK_SIZE + 1) as u64, reader);
    let write =
        tokio::spawn(async move { home.write("mutable/cancel", body, &no_progress()).await });
    waiting.notified().await;

    write.abort();
    assert!(write.await.expect_err("write task canceled").is_cancelled());
    release.notify_waiters();

    let part_key = chunk_part_key("mutable/cancel", "cloudkit-upload-0", 0);
    assert!(!ops
        .record_exists(&CloudKitScope::Private, &part_key)
        .expect("inspect canceled part"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_after_mutable_manifest_publish_preserves_the_committed_layout() {
    let (home, ops) = make_cloud_home_with_ops();
    let key = "mutable/published";
    let data = vec![9; CHUNK_SIZE + 1];
    let (stored, release) = ops.pause_write_after_store(&chunk_manifest_key(key));
    let write_home = home.clone();
    let write_data = data.clone();
    let write = tokio::spawn(async move {
        write_home
            .write(key, BlobBody::from_bytes(write_data), &no_progress())
            .await
    });
    tokio::task::spawn_blocking(move || stored.wait())
        .await
        .expect("wait for manifest publication");

    write.abort();
    tokio::task::spawn_blocking(move || release.wait())
        .await
        .expect("release manifest publication");
    assert!(write.await.expect_err("write task canceled").is_cancelled());

    assert_eq!(home.read(key).await.expect("read committed layout"), data);
    assert!(ops
        .record_exists(&CloudKitScope::Private, &chunk_manifest_key(key))
        .expect("inspect committed manifest"));
    assert_eq!(
        ops.list_records(&CloudKitScope::Private, &format!("{key}.part"))
            .expect("inspect committed parts")
            .len(),
        2
    );
}

#[test]
fn mutable_cancellation_cleanup_failure_terminates_the_process() {
    const CHILD: &str = "COVEN_CLOUDKIT_MUTABLE_CANCEL_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Runtime::new().expect("build child runtime");
        runtime.block_on(async {
            let (home, ops) = make_cloud_home_with_ops();
            let mut sink = home
                .open_multipart("mutable/cancel", (CHUNK_SIZE + 1) as u64)
                .await
                .expect("open CloudKit multipart upload");
            sink.send_part(Bytes::from(vec![7; CHUNK_SIZE]), 0, false)
                .await
                .expect("write first multipart part");
            ops.fail_delete(&chunk_part_key("mutable/cancel", "cloudkit-upload-0", 0));
            drop(sink);
        });
        std::process::exit(0);
    }

    let status = std::process::Command::new(
        std::env::current_exe().expect("locate CloudKit test executable"),
    )
    .arg("mutable_cancellation_cleanup_failure_terminates_the_process")
    .arg("--nocapture")
    .env(CHILD, "1")
    .status()
    .expect("run CloudKit mutable cancellation subprocess");
    assert!(!status.success(), "cancellation subprocess survived");
}

#[tokio::test]
async fn write_reports_progress_per_chunk_record() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let ch = make_cloud_home();
    // 25 MB spans three records (10 + 10 + 5) so progress fires three
    // times, the last equalling the total.
    let total = 25 * 1024 * 1024u64;
    let data: Vec<u8> = vec![0u8; total as usize];
    let last = Arc::new(AtomicU64::new(0));
    let ticks = Arc::new(AtomicU64::new(0));
    let last2 = last.clone();
    let ticks2 = ticks.clone();
    let sink = move |n: u64| {
        last2.store(n, Ordering::Relaxed);
        ticks2.fetch_add(1, Ordering::Relaxed);
    };
    ch.write("chunked.bin", BlobBody::from_bytes(data), &sink)
        .await
        .unwrap();
    assert_eq!(last.load(Ordering::Relaxed), total);
    assert_eq!(ticks.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_small_file_roundtrip() {
    let ch = make_cloud_home();
    let data = b"hello world".to_vec();
    ch.write(
        "small.bin",
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();
    let read = ch.read("small.bin").await.unwrap();
    assert_eq!(read, data);
}

#[tokio::test]
async fn test_large_file_roundtrip() {
    let ch = make_cloud_home();
    // 25MB of data -- spans 3 chunks (10 + 10 + 5)
    let data: Vec<u8> = (0..25 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    ch.write(
        "large.bin",
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();
    let read = ch.read("large.bin").await.unwrap();
    assert_eq!(read.len(), data.len());
    assert_eq!(read, data);
}

#[tokio::test]
async fn test_read_range_single() {
    let ch = make_cloud_home();
    ch.write(
        "range.bin",
        BlobBody::from_bytes(b"0123456789".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    let slice = ch.read_range("range.bin", 3, 7).await.unwrap();
    assert_eq!(slice, b"3456");
}

#[tokio::test]
async fn read_single_record_does_not_probe_existence() {
    let (ch, ops) = make_cloud_home_with_ops();
    let data = b"hello world".to_vec();
    ch.write(
        "single.bin",
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    let read = ch.read("single.bin").await.unwrap();

    assert_eq!(read, data);
    assert_eq!(ops.record_exists_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn read_range_single_record_does_not_probe_existence() {
    let (ch, ops) = make_cloud_home_with_ops();
    ch.write(
        "single-range.bin",
        BlobBody::from_bytes(b"0123456789".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let read = ch.read_range("single-range.bin", 2, 6).await.unwrap();

    assert_eq!(read, b"2345");
    assert_eq!(ops.record_exists_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn read_chunked_record_without_manifest_errors() {
    let (ch, ops) = make_cloud_home_with_ops();
    let first = vec![1u8; CHUNK_SIZE];
    let second = b"tail".to_vec();
    ops.write_chunk_part("chunked.bin", 0, first.clone());
    ops.write_chunk_part("chunked.bin", 1, second.clone());

    let err = ch
        .read("chunked.bin")
        .await
        .expect_err("chunks without manifest must fail");
    let msg = err.to_string();

    assert!(
        msg.contains("chunked.bin") && msg.contains("no manifest"),
        "unexpected error: {msg}"
    );
    assert!(!ch.exists("chunked.bin").await.unwrap());
}

#[tokio::test]
async fn list_omits_base_key_whose_manifest_is_absent() {
    let (ch, ops) = make_cloud_home_with_ops();
    // Part records with no manifest — an interrupted upload that never
    // published. `read` cannot assemble them, so `list` must not report them.
    ops.write_chunk_part("files/orphan.bin", 0, vec![1u8; CHUNK_SIZE]);
    ops.write_chunk_part("files/orphan.bin", 1, b"tail".to_vec());
    ch.write(
        "files/ok.bin",
        BlobBody::from_bytes(b"hi".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let keys = ch.list("files/").await.unwrap();

    assert_eq!(keys, vec!["files/ok.bin".to_string()]);
}

#[tokio::test]
async fn multipart_part_failure_leaves_no_orphan_records_or_visibility() {
    let (ch, ops) = make_cloud_home_with_ops();
    // 25 MB spans three parts; fail the second part write mid-upload. The
    // upload id is the first id the sequential provider hands out.
    ops.fail_write(&chunk_part_key("orphan.bin", "cloudkit-upload-0", 1));
    let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];

    let err = ch
        .write("orphan.bin", BlobBody::from_bytes(data), &no_progress())
        .await
        .expect_err("injected part write failure must fail the upload");
    assert!(err.to_string().contains("write"), "unexpected error: {err}");

    assert!(!ch.exists("orphan.bin").await.unwrap());
    assert!(!ch
        .list("")
        .await
        .unwrap()
        .contains(&"orphan.bin".to_string()));
    assert!(
        ops.list_records(&CloudKitScope::Private, "orphan.bin.part")
            .unwrap()
            .is_empty(),
        "aborted upload must leave no part records"
    );
    assert!(!ops
        .record_exists(&CloudKitScope::Private, &chunk_manifest_key("orphan.bin"))
        .unwrap());
}

#[tokio::test]
async fn read_chunked_record_with_missing_manifest_part_errors() {
    let (ch, ops) = make_cloud_home_with_ops();
    let first = vec![1u8; CHUNK_SIZE];
    let second = vec![2u8; CHUNK_SIZE];
    let total_len = (CHUNK_SIZE * 2) + 4;
    ops.write_chunk_manifest("chunked.bin", total_len);
    ops.write_chunk_part("chunked.bin", 0, first);
    ops.write_chunk_part("chunked.bin", 1, second);

    let err = ch
        .read("chunked.bin")
        .await
        .expect_err("missing manifest part must fail");
    let msg = err.to_string();

    assert!(
        msg.contains("expects 3 parts") && msg.contains("found 2"),
        "unexpected error: {msg}"
    );
    assert!(!ch.exists("chunked.bin").await.unwrap());
}

#[tokio::test]
async fn read_range_chunked_rejects_range_past_manifest_length() {
    let ch = make_cloud_home();
    let data: Vec<u8> = vec![7u8; 15 * 1024 * 1024];
    ch.write(
        "range-limit.bin",
        BlobBody::from_bytes(data),
        &no_progress(),
    )
    .await
    .unwrap();

    let err = ch
        .read_range("range-limit.bin", 0, (16 * 1024 * 1024) as u64)
        .await
        .expect_err("range past manifest length must fail");
    let msg = err.to_string();

    assert!(msg.contains("exceeds file size"), "unexpected error: {msg}");
}

#[tokio::test]
async fn read_range_chunked_short_chunk_errors_instead_of_panicking() {
    let (ch, ops) = make_cloud_home_with_ops();
    let total_len = CHUNK_SIZE + 8;
    ops.write_chunk_manifest("short-tail.bin", total_len);
    ops.write_chunk_part("short-tail.bin", 0, vec![1u8; CHUNK_SIZE]);
    ops.write_chunk_part("short-tail.bin", 1, vec![2u8; 4]);

    let err = ch
        .read_range("short-tail.bin", CHUNK_SIZE as u64, (CHUNK_SIZE + 8) as u64)
        .await
        .expect_err("short tail chunk must fail");
    let msg = err.to_string();

    assert!(
        msg.contains("part 1") && msg.contains("expected 8"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn test_read_range_chunked() {
    let ch = make_cloud_home();
    // Create data that spans 2 chunks: 15MB
    let data: Vec<u8> = (0..15 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    ch.write(
        "big.bin",
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    // Read a range that crosses the chunk boundary (last byte of chunk 0, first byte of chunk 1)
    let boundary = CHUNK_SIZE;
    let start = (boundary - 2) as u64;
    let end = (boundary + 3) as u64;
    let slice = ch.read_range("big.bin", start, end).await.unwrap();
    assert_eq!(slice.len(), 5);
    assert_eq!(slice, &data[start as usize..end as usize]);
}

#[tokio::test]
async fn test_list_deduplicates_chunks() {
    let ch = make_cloud_home();
    // Write a chunked file
    let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
    ch.write(
        "files/album.flac",
        BlobBody::from_bytes(data),
        &no_progress(),
    )
    .await
    .unwrap();

    // Also write a small file
    ch.write(
        "files/cover.jpg",
        BlobBody::from_bytes(b"img".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let keys = ch.list("files/").await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"files/album.flac".to_string()));
    assert!(keys.contains(&"files/cover.jpg".to_string()));
}

#[tokio::test]
async fn test_delete_removes_all_chunks() {
    let ch = make_cloud_home();
    let data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
    ch.write("to-delete.bin", BlobBody::from_bytes(data), &no_progress())
        .await
        .unwrap();

    assert!(ch.exists("to-delete.bin").await.unwrap());

    ch.delete("to-delete.bin").await.unwrap();

    assert!(!ch.exists("to-delete.bin").await.unwrap());

    // Verify the underlying ops store is empty of related keys
    let ops = &ch.ops;
    let keys = ops
        .list_records(&CloudKitScope::Private, "to-delete.bin")
        .unwrap();
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_overwrite_chunked_with_single() {
    let ch = make_cloud_home();
    // Write large file (chunked)
    let large_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
    ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
        .await
        .unwrap();

    // Overwrite with small file (single record)
    let small_data = b"small".to_vec();
    ch.write(
        "file.bin",
        BlobBody::from_bytes(small_data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    let read = ch.read("file.bin").await.unwrap();
    assert_eq!(read, small_data);

    // Verify no chunk records remain
    let chunks = ch
        .ops
        .list_records(&CloudKitScope::Private, "file.bin.part")
        .unwrap();
    assert!(chunks.is_empty());
}

#[tokio::test]
async fn put_object_over_single_writes_without_deleting_base_first() {
    let (ch, ops) = make_cloud_home_with_ops();
    ch.write(
        "file.bin",
        BlobBody::from_bytes(b"old".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    ops.clear_calls();

    ch.write(
        "file.bin",
        BlobBody::from_bytes(b"new".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let calls = ops.calls();
    assert_eq!(
        calls.first(),
        Some(&MockCall::Write("file.bin".to_string()))
    );
    assert!(
        !calls.contains(&MockCall::Delete("file.bin".to_string())),
        "single-record overwrite must not delete the base record: {calls:?}"
    );
    assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
}

#[tokio::test]
async fn put_object_over_chunked_publishes_single_before_cleanup() {
    let (ch, ops) = make_cloud_home_with_ops();
    let large_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
    ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
        .await
        .unwrap();
    ops.clear_calls();

    ch.write(
        "file.bin",
        BlobBody::from_bytes(b"new".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let calls = ops.calls();
    assert_eq!(
        calls.first(),
        Some(&MockCall::Write("file.bin".to_string()))
    );
    assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
    assert!(ch
        .ops
        .list_records(&CloudKitScope::Private, "file.bin.part")
        .unwrap()
        .is_empty());
    assert!(!ch
        .ops
        .record_exists(&CloudKitScope::Private, &chunk_manifest_key("file.bin"))
        .unwrap());
}

#[tokio::test]
async fn put_object_cleanup_failure_leaves_new_single_readable() {
    let (ch, ops) = make_cloud_home_with_ops();
    let large_data: Vec<u8> = vec![0u8; 15 * 1024 * 1024];
    ch.write("file.bin", BlobBody::from_bytes(large_data), &no_progress())
        .await
        .unwrap();
    let stale_chunk = ch
        .ops
        .list_records(&CloudKitScope::Private, "file.bin.part")
        .unwrap()
        .into_iter()
        .next()
        .expect("chunked setup writes a chunk");
    ops.fail_delete(&stale_chunk);

    let err = ch
        .write(
            "file.bin",
            BlobBody::from_bytes(b"new".to_vec()),
            &no_progress(),
        )
        .await
        .expect_err("stale chunk cleanup failure must fail loud");
    let msg = err.to_string();

    assert!(msg.contains("delete"), "unexpected error: {msg}");
    assert_eq!(ch.read("file.bin").await.unwrap(), b"new");
}

#[tokio::test]
async fn test_overwrite_single_with_chunked() {
    let (ch, ops) = make_cloud_home_with_ops();
    // Write small file
    ch.write(
        "file.bin",
        BlobBody::from_bytes(b"small".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    ops.clear_calls();

    // Overwrite with large file (chunked)
    let large_data: Vec<u8> = vec![1u8; 25 * 1024 * 1024];
    ch.write(
        "file.bin",
        BlobBody::from_bytes(large_data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    let read = ch.read("file.bin").await.unwrap();
    assert_eq!(read, large_data);

    let calls = ops.calls();
    let manifest_write = calls
        .iter()
        .position(|call| *call == MockCall::Write(chunk_manifest_key("file.bin")))
        .expect("chunked write publishes manifest");
    let base_delete = calls
        .iter()
        .position(|call| *call == MockCall::Delete("file.bin".to_string()))
        .expect("chunked write removes stale single base");
    assert!(
        manifest_write < base_delete,
        "chunk manifest must publish before stale base cleanup: {calls:?}"
    );

    // The single-record base is replaced by the chunk layout.
    assert!(!ch
        .ops
        .record_exists(&CloudKitScope::Private, "file.bin")
        .unwrap());
    assert!(ch
        .ops
        .record_exists(&CloudKitScope::Private, &chunk_manifest_key("file.bin"))
        .unwrap());
}

#[tokio::test]
async fn chunked_over_longer_chunked_uses_new_token_before_stale_cleanup() {
    let (ch, ops) = make_cloud_home_with_ops();
    let old_data: Vec<u8> = vec![0u8; 25 * 1024 * 1024];
    ch.write("file.bin", BlobBody::from_bytes(old_data), &no_progress())
        .await
        .unwrap();
    let old_chunks = ops
        .list_records(&CloudKitScope::Private, "file.bin.part")
        .unwrap();
    ops.clear_calls();

    let new_data: Vec<u8> = vec![1u8; 15 * 1024 * 1024];
    ch.write(
        "file.bin",
        BlobBody::from_bytes(new_data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    assert_eq!(ch.read("file.bin").await.unwrap(), new_data);
    let remaining_chunks = ch
        .ops
        .list_records(&CloudKitScope::Private, "file.bin.part")
        .unwrap();
    assert_eq!(remaining_chunks.len(), 2);
    assert!(
        old_chunks
            .iter()
            .all(|old| !remaining_chunks.iter().any(|new| new == old)),
        "old token chunks must be cleaned after new manifest publishes"
    );
}

#[tokio::test]
async fn test_exists() {
    let ch = make_cloud_home();

    assert!(!ch.exists("nope.bin").await.unwrap());

    ch.write(
        "yep.bin",
        BlobBody::from_bytes(b"data".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();
    assert!(ch.exists("yep.bin").await.unwrap());

    // Chunked file
    let data: Vec<u8> = vec![0u8; 15 * 1024 * 1024];
    ch.write("chunked.bin", BlobBody::from_bytes(data), &no_progress())
        .await
        .unwrap();
    assert!(ch.exists("chunked.bin").await.unwrap());
}

#[tokio::test]
async fn test_read_range_empty_when_end_leq_start() {
    let ch = make_cloud_home();
    ch.write(
        "range.bin",
        BlobBody::from_bytes(b"0123456789".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    // end == start returns empty
    let slice = ch.read_range("range.bin", 3, 3).await.unwrap();
    assert!(slice.is_empty());

    // end < start returns empty
    let slice = ch.read_range("range.bin", 5, 2).await.unwrap();
    assert!(slice.is_empty());

    // end == 0 returns empty (the underflow case)
    let slice = ch.read_range("range.bin", 0, 0).await.unwrap();
    assert!(slice.is_empty());
}

/// The O(range) receipt for the backend the app actually ships on. CloudKit
/// stores an exact object as a manifest plus numbered part records, so a
/// ranged read must fetch the manifest and only the parts covering the
/// range. Reading the whole object and slicing answers correctly and costs
/// the object — the sabotage this test exists to catch, since a caller that
/// fetches only covering chunks gains nothing if the backend under it reads
/// everything anyway.
#[tokio::test]
async fn exact_ranged_read_fetches_only_the_parts_it_covers() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("audio/ranged-track");
    // Four parts: three full chunks and a short tail.
    let data: Vec<u8> = (0..3 * CHUNK_SIZE + 1024)
        .map(|value| (value % 251) as u8)
        .collect();
    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    // A range wholly inside part 2.
    ops.clear_versioned_reads();
    let start = 2 * CHUNK_SIZE + 10;
    let end = start + 64;
    assert_eq!(
        ExactSlotStorage::read_range_at(&home, &slot, start as u64, end as u64)
            .await
            .unwrap(),
        &data[start..end],
    );
    assert_eq!(
        ops.versioned_reads(),
        vec![
            "audio/ranged-track".to_string(),
            "audio/ranged-track.exact-part2".to_string(),
        ],
        "the manifest names the layout; only the covering part is fetched",
    );

    // A range straddling the part 0 / part 1 boundary fetches exactly two.
    ops.clear_versioned_reads();
    let start = CHUNK_SIZE - 8;
    let end = CHUNK_SIZE + 8;
    assert_eq!(
        ExactSlotStorage::read_range_at(&home, &slot, start as u64, end as u64)
            .await
            .unwrap(),
        &data[start..end],
    );
    assert_eq!(
        ops.versioned_reads(),
        vec![
            "audio/ranged-track".to_string(),
            "audio/ranged-track.exact-part0".to_string(),
            "audio/ranged-track.exact-part1".to_string(),
        ],
    );

    // The tail, in the short last part.
    ops.clear_versioned_reads();
    assert_eq!(
        ExactSlotStorage::read_range_at(&home, &slot, data.len() as u64 - 16, data.len() as u64)
            .await
            .unwrap(),
        &data[data.len() - 16..],
    );
    assert_eq!(
        ops.versioned_reads(),
        vec![
            "audio/ranged-track".to_string(),
            "audio/ranged-track.exact-part3".to_string(),
        ],
    );

    // The whole read is the one that legitimately touches every part, so the
    // counter discriminates rather than just being small.
    ops.clear_versioned_reads();
    assert_eq!(ExactSlotStorage::read_at(&home, &slot).await.unwrap(), data);
    assert_eq!(
        ops.versioned_reads().len(),
        5,
        "a whole read fetches the manifest and all four parts",
    );

    // A range past the end is refused rather than shortened.
    assert!(ExactSlotStorage::read_range_at(
        &home,
        &slot,
        data.len() as u64 - 4,
        data.len() as u64 + 4,
    )
    .await
    .is_err());
    assert!(ExactSlotStorage::read_range_at(&home, &slot, 10, 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn exact_bounded_records_are_create_only() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/bounded");
    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(b"first".to_vec()),
        &no_progress(),
    )
    .await
    .unwrap();

    let collision = ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(b"second".to_vec()),
        &no_progress(),
    )
    .await
    .expect_err("an immutable record must never overwrite an existing key");
    assert!(matches!(collision, CloudHomeError::AlreadyExists(key) if key == "copies/bounded"));
    assert_eq!(
        ExactSlotStorage::read_at(&home, &slot).await.unwrap(),
        b"first"
    );

    ops.write_record(
        &CloudKitScope::Private,
        "copies/bounded",
        b"replacement".to_vec(),
    )
    .unwrap();
    let changed = ExactSlotStorage::read_at(&home, &slot)
        .await
        .expect_err("an exact read must reject a replaced manifest");
    assert!(changed.to_string().contains("invalid manifest"));
}

#[tokio::test]
async fn exact_multipart_stages_one_bounded_part_at_a_time_and_manifest_last() {
    let (home, ops) = make_cloud_home_with_ops();
    let data = vec![7u8; CHUNK_SIZE + 13];
    let slot = exact_slot("copies/chunked");

    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .unwrap();

    assert_eq!(
        ops.calls(),
        vec![
            MockCall::BeginBatch("batch-0".to_string()),
            MockCall::Stage(exact_part_key("copies/chunked", 0)),
            MockCall::Stage(exact_part_key("copies/chunked", 1)),
            MockCall::Stage("copies/chunked".to_string()),
            MockCall::CommitBatch("batch-0".to_string()),
        ]
    );
    assert_eq!(ops.max_stage_payload.load(Ordering::SeqCst), CHUNK_SIZE);
    assert_eq!(ExactSlotStorage::read_at(&home, &slot).await.unwrap(), data);

    let manifest_bytes = ops
        .read_record(&CloudKitScope::Private, "copies/chunked")
        .unwrap();
    assert_eq!(
        decode_exact_manifest(&manifest_bytes).unwrap(),
        (2, data.len())
    );
}

#[tokio::test]
async fn lost_atomic_commit_response_is_settled_by_readback() {
    let (home, ops) = make_cloud_home_with_ops();
    ops.lose_commit_response();
    let data = vec![2u8; CHUNK_SIZE + 1];
    let slot = exact_slot("copies/ambiguous");

    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .expect("authoritative readback settles a committed create");

    assert_eq!(ExactSlotStorage::read_at(&home, &slot).await.unwrap(), data);
}

#[tokio::test]
async fn concurrent_immutable_creates_have_one_winner() {
    let (home, _) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/create-race");
    let left_progress = no_progress();
    let right_progress = no_progress();

    let (left, right) = tokio::join!(
        ExactSlotStorage::create_at(
            &home,
            &slot,
            BlobBody::from_bytes(b"left".to_vec()),
            &left_progress,
        ),
        ExactSlotStorage::create_at(
            &home,
            &slot,
            BlobBody::from_bytes(b"right".to_vec()),
            &right_progress,
        ),
    );

    assert!(matches!(
        (&left, &right),
        (Ok(()), Err(CloudHomeError::AlreadyExists(_)))
            | (Err(CloudHomeError::AlreadyExists(_)), Ok(()))
    ));
    let expected = if left.is_ok() {
        b"left".as_slice()
    } else {
        b"right".as_slice()
    };
    assert_eq!(
        ExactSlotStorage::read_at(&home, &slot).await.unwrap(),
        expected
    );
}

#[tokio::test]
async fn mismatched_commit_keys_are_checked_against_authoritative_records() {
    let (home, ops) = make_cloud_home_with_ops();
    ops.return_wrong_commit_keys();
    let data = vec![3u8; CHUNK_SIZE + 1];
    let slot = exact_slot("copies/locator-mismatch");

    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(data.clone()),
        &no_progress(),
    )
    .await
    .expect("authoritative reads verify committed records");

    assert_eq!(ExactSlotStorage::read_at(&home, &slot).await.unwrap(), data);
}

#[tokio::test]
async fn immutable_atomic_multipart_failure_and_collision_create_no_partial_layout() {
    let (home, ops) = make_cloud_home_with_ops();
    let first_part = exact_part_key("copies/failed", 0);
    let second_part = exact_part_key("copies/failed", 1);
    ops.fail_write(&second_part);
    let data = vec![3u8; CHUNK_SIZE + 1];
    let slot = exact_slot("copies/failed");

    let error =
        ExactSlotStorage::create_at(&home, &slot, BlobBody::from_bytes(data), &no_progress())
            .await
            .expect_err("part creation failure must abort the append");
    assert!(matches!(error, CloudHomeError::Transport(_)));
    assert!(!ops
        .record_exists(&CloudKitScope::Private, &first_part)
        .unwrap());
    assert!(!ops
        .record_exists(&CloudKitScope::Private, "copies/failed")
        .unwrap());
    assert!(ops.staged_batches.lock().unwrap().is_empty());

    let (home, ops) = make_cloud_home_with_ops();
    let first_part = exact_part_key("copies/collision", 0);
    let second_part = exact_part_key("copies/collision", 1);
    ops.write_record(&CloudKitScope::Private, &second_part, b"existing".to_vec())
        .unwrap();
    let slot = exact_slot("copies/collision");
    let error = ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(vec![3u8; CHUNK_SIZE + 1]),
        &no_progress(),
    )
    .await
    .expect_err("a batch collision must reject the whole append");
    assert!(matches!(error, CloudHomeError::AlreadyExists(key) if key == "copies/collision"));
    assert!(!ops
        .record_exists(&CloudKitScope::Private, &first_part)
        .unwrap());
    assert!(!ops
        .record_exists(&CloudKitScope::Private, "copies/collision")
        .unwrap());
    assert!(ops
        .record_exists(&CloudKitScope::Private, &second_part)
        .unwrap());
    assert!(ops.staged_batches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn immutable_staging_cleanup_failure_is_typed_and_remote_state_stays_empty() {
    let (home, ops) = make_cloud_home_with_ops();
    let second_part = exact_part_key("copies/discard", 1);
    ops.fail_write(&second_part);
    ops.fail_discard();
    let slot = exact_slot("copies/discard");

    let error = ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(vec![4u8; CHUNK_SIZE + 1]),
        &no_progress(),
    )
    .await
    .expect_err("failed staging discard must be returned with the commit error");

    assert!(matches!(error, CloudHomeError::CleanupFailed { .. }));
    assert!(error.to_string().contains("batch-0"), "{error}");
    assert!(ops.store.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dropping_an_uncommitted_staging_batch_discards_host_local_payloads() {
    let (home, ops) = make_cloud_home_with_ops();
    let staging = home.begin_atomic_create().await.unwrap();
    staging
        .clone()
        .stage_record(CloudKitRecordCreate {
            key: "copies/cancelled.part0.upload".to_string(),
            data: vec![8u8; CHUNK_SIZE],
        })
        .await
        .unwrap();

    drop(staging);

    assert!(ops.staged_batches.lock().unwrap().is_empty());
    assert!(ops.store.lock().unwrap().is_empty());
    assert!(ops
        .calls()
        .contains(&MockCall::DiscardBatch("batch-0".to_string())));
}

#[test]
fn cancellation_discard_failure_terminates_the_process() {
    const CHILD: &str = "COVEN_CLOUDKIT_CANCEL_DISCARD_ABORT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ops = Arc::new(MockCloudKitOps::new());
            let home = CloudKitCloudHome::new_private(ops.clone());
            let staging = home.begin_atomic_create().await.unwrap();
            staging
                .clone()
                .stage_record(CloudKitRecordCreate {
                    key: "copies/cancelled.part0.upload".to_string(),
                    data: vec![8u8; CHUNK_SIZE],
                })
                .await
                .unwrap();
            ops.fail_discard();
            let started = Arc::new(std::sync::Barrier::new(2));
            let release = Arc::new(std::sync::Barrier::new(2));
            let worker_started = started.clone();
            let worker_release = release.clone();
            let owner = tokio::spawn(async move {
                tokio::task::spawn_blocking(move || {
                    worker_started.wait();
                    worker_release.wait();
                    drop(staging);
                })
                .await
                .unwrap();
            });
            started.wait();
            owner.abort();
            release.wait();
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        });
        panic!("failed cancellation discard did not abort the process");
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("cancellation_discard_failure_terminates_the_process")
        .arg("--nocapture")
        .env(CHILD, "1")
        .status()
        .expect("run CloudKit cancellation sabotage subprocess");
    assert!(
        !status.success(),
        "sabotage subprocess unexpectedly survived"
    );
}

#[tokio::test]
async fn exact_delete_removes_the_manifest_and_every_part() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/delete");
    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(vec![5u8; CHUNK_SIZE + 1]),
        &no_progress(),
    )
    .await
    .unwrap();

    ExactSlotStorage::delete_at(&home, &slot)
        .await
        .expect("delete exact slot");

    for key in [
        "copies/delete".to_string(),
        exact_part_key("copies/delete", 0),
        exact_part_key("copies/delete", 1),
    ] {
        assert!(!ops.record_exists(&CloudKitScope::Private, &key).unwrap());
    }
}

#[tokio::test]
async fn grant_access_returns_share_join_info_without_email() {
    let ch = make_cloud_home();
    // CloudKit shares bind identity at URL-accept time, so no invitee email
    // is supplied and the grant still succeeds.
    let join_info = ch
        .set_access(CloudAccessState::Present {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        })
        .await
        .unwrap();
    assert_eq!(
        join_info,
        CloudAccessOutcome::Present(CloudHomeJoinInfo::CloudKitShare {
            share_url: "https://share.example/member-pubkey".to_string(),
            owner_name: "owner-name".to_string(),
            zone_name: "bae-store".to_string(),
        })
    );
}

#[tokio::test]
async fn revoke_access_unshares_and_reports_revoked() {
    let ch = make_cloud_home();
    let outcome = ch
        .set_access(CloudAccessState::Absent {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        })
        .await
        .unwrap();
    // CloudKit removes the member's share participation, so it reports the
    // credential actually withdrawn rather than Unsupported.
    assert_eq!(outcome, CloudAccessOutcome::Absent(RevokeOutcome::Revoked));
}

#[tokio::test]
async fn repeated_present_access_reuses_the_verified_share() {
    let (home, ops) = make_cloud_home_with_ops();
    let desired = CloudAccessState::Present {
        member_pubkey: "member-pubkey".to_string(),
        provider_account_email: None,
    };

    let first = home.set_access(desired.clone()).await.unwrap();
    let second = home.set_access(desired).await.unwrap();

    assert_eq!(first, second);
    assert_eq!(ops.grant_share_calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn repeated_absent_access_does_not_revoke_twice() {
    let (home, ops) = make_cloud_home_with_ops();
    home.set_access(CloudAccessState::Present {
        member_pubkey: "member-pubkey".to_string(),
        provider_account_email: None,
    })
    .await
    .unwrap();
    let desired = CloudAccessState::Absent {
        member_pubkey: "member-pubkey".to_string(),
        provider_account_email: None,
    };

    home.set_access(desired.clone()).await.unwrap();
    home.set_access(desired).await.unwrap();

    assert_eq!(ops.revoke_share_calls.load(Ordering::Relaxed), 1);
}

#[test]
fn test_strip_part_suffix() {
    assert_eq!(strip_part_suffix("file.bin.part0"), "file.bin");
    assert_eq!(strip_part_suffix("file.bin.part123"), "file.bin");
    assert_eq!(
        strip_part_suffix("file.bin.part123.0123456789abcdef0123456789abcdef"),
        "file.bin"
    );
    assert_eq!(strip_part_suffix("file.bin.manifest"), "file.bin");
    assert_eq!(strip_part_suffix("file.bin"), "file.bin");
    assert_eq!(strip_part_suffix("file.partition"), "file.partition");
    assert_eq!(strip_part_suffix("file.part"), "file.part"); // no digits after .part
}
