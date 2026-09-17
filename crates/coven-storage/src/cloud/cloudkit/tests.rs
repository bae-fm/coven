use super::exact::*;
use super::*;
use crate::cloud::no_progress;
use crate::cloud::{ExactUpload, UploadControl};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

fn exact_slot(key: &str) -> ObjectSlot {
    ObjectSlot::logical(key.to_string()).expect("valid exact slot")
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MockCall {
    List(String),
    Delete(String),
    Exists(String),
    BeginBatch(String),
    Stage(String),
    CommitBatch(String),
    DiscardBatch(String),
    DeleteVersions(Vec<String>),
}

struct MockCloudKitOps {
    store: Mutex<HashMap<(CloudKitScope, String), Vec<u8>>>,
    versions: Mutex<HashMap<(CloudKitScope, String), u64>>,
    calls: Mutex<Vec<MockCall>>,
    fail_deletes: Mutex<HashSet<String>>,
    retain_on_delete: Mutex<HashSet<String>>,
    fail_delete_once: Mutex<HashMap<String, usize>>,
    fail_writes: Mutex<HashSet<String>>,
    staged_batches: Mutex<HashMap<String, Vec<CloudKitRecordCreate>>>,
    next_batch: AtomicUsize,
    max_stage_payload: AtomicUsize,
    fail_discards: AtomicBool,
    lose_commit_response: AtomicBool,
    return_wrong_commit_keys: AtomicBool,
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
            retain_on_delete: Mutex::new(HashSet::new()),
            fail_delete_once: Mutex::new(HashMap::new()),
            fail_writes: Mutex::new(HashSet::new()),
            staged_batches: Mutex::new(HashMap::new()),
            next_batch: AtomicUsize::new(0),
            max_stage_payload: AtomicUsize::new(0),
            fail_discards: AtomicBool::new(false),
            lose_commit_response: AtomicBool::new(false),
            return_wrong_commit_keys: AtomicBool::new(false),
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

    /// Acknowledge deletion of `key` without performing it — a provider that
    /// reports success and keeps the record. Proving a slot absent exists to
    /// catch exactly this, and nothing else can produce it.
    fn retain_on_delete(&self, key: &str) {
        self.retain_on_delete
            .lock()
            .unwrap()
            .insert(key.to_string());
    }

    /// Put a record in the zone behind the adapter's back, so a test can drive
    /// what an exact operation does about a record it did not write — a slot a
    /// competing writer already occupies, or a manifest something replaced.
    fn seed_record(&self, key: &str, data: Vec<u8>) {
        let record = (CloudKitScope::Private, key.to_string());
        self.store.lock().unwrap().insert(record.clone(), data);
        let mut versions = self.versions.lock().unwrap();
        let next = versions.get(&record).copied().unwrap_or(0) + 1;
        versions.insert(record, next);
    }

    /// The bytes the zone currently holds at `key`, without going through the
    /// adapter, so a test can assert on the stored representation itself.
    fn stored_record(&self, key: &str) -> Vec<u8> {
        self.store
            .lock()
            .unwrap()
            .get(&(CloudKitScope::Private, key.to_string()))
            .cloned()
            .unwrap_or_else(|| panic!("no CloudKit record at {key}"))
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
        if self.retain_on_delete.lock().unwrap().contains(key) {
            return Ok(());
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

    fn replace_record_if_version(
        &self,
        scope: &CloudKitScope,
        key: &str,
        expected: &CloudObjectVersion,
        data: Vec<u8>,
    ) -> Result<ConditionalWriteOutcome, CloudHomeError> {
        let record = (scope.clone(), key.to_string());
        let mut store = self.store.lock().unwrap();
        let mut versions = self.versions.lock().unwrap();
        let current = versions
            .get(&record)
            .copied()
            .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))?;
        if current.to_string() != expected.as_provider() {
            return Ok(ConditionalWriteOutcome::VersionChanged);
        }
        let next = current
            .checked_add(1)
            .expect("mock CloudKit record version overflow");
        store.insert(record.clone(), data);
        versions.insert(record, next);
        Ok(ConditionalWriteOutcome::Replaced(
            CloudObjectVersion::from_provider(next.to_string())?,
        ))
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
        let retained = self.retain_on_delete.lock().unwrap();
        for record in records {
            if retained.contains(&record.key) {
                continue;
            }
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
    CloudKitCloudHome::new_private(
        Arc::new(MockCloudKitOps::new()),
        coven_foundation::config::ExactUploadVerification::MetadataHash,
    )
}

fn make_cloud_home_with_ops() -> (CloudKitCloudHome, Arc<MockCloudKitOps>) {
    let ops = Arc::new(MockCloudKitOps::new());
    (
        CloudKitCloudHome::new_private(
            ops.clone(),
            coven_foundation::config::ExactUploadVerification::MetadataHash,
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

/// A listing names the object slots CloudKit holds. An exact object's parts
/// are reachable only through the manifest record at its own slot, so the
/// listing reports that slot and never the `.exact-part` records beside it.
#[tokio::test]
async fn list_slots_reports_base_slots_and_omits_exact_parts() {
    let (ch, _ops) = make_cloud_home_with_ops();
    for (key, bytes) in [
        ("files/album.flac", vec![0u8; 25 * 1024 * 1024]),
        ("files/cover.jpg", b"img".to_vec()),
    ] {
        crate::cloud::create_exact_bytes(&ch, &exact_slot(key), &bytes, &no_progress())
            .await
            .expect("create exact object");
    }

    let listed = ch.list_slots("files/").await.expect("list exact slots");

    // A provider's listing order is its own, so the assertion is on which
    // slots came back, not the sequence they arrived in.
    let mut keys = listed
        .iter()
        .map(|slot| slot.logical_key().to_string())
        .collect::<Vec<_>>();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "files/album.flac".to_string(),
            "files/cover.jpg".to_string()
        ]
    );
}

/// An empty range is the caller asking for nothing, which costs no record
/// read rather than erroring on a manifest it never needed.
#[tokio::test]
async fn exact_ranged_read_is_empty_when_end_equals_start() {
    let (ch, _ops) = make_cloud_home_with_ops();
    let slot = exact_slot("range.bin");
    crate::cloud::create_exact_bytes(&ch, &slot, b"0123456789", &no_progress())
        .await
        .expect("create exact object");

    assert!(ch.read_range_at(&slot, 3, 3).await.unwrap().is_empty());
}

/// The O(range) receipt for the backend the app actually ships on. CloudKit
/// stores an exact object as a manifest plus numbered part records, so a
/// ranged read must fetch the manifest and only the parts covering the
/// range. Reading the whole object and slicing answers correctly and costs
/// the object — the sabotage this test exists to catch, since a caller that
/// fetches only covering chunks gains nothing if the backend under it reads
/// everything anyway.
/// The stream fetches one part record at a time, in order, with no
/// whole-object buffer standing between the provider and the reader.
#[tokio::test]
async fn exact_stream_serves_a_multi_part_body_one_part_at_a_time() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("audio/streamed-track");
    let data: Vec<u8> = (0..CHUNK_SIZE + 1024)
        .map(|value| (value % 251) as u8)
        .collect();
    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
        .await
        .unwrap();

    ops.clear_versioned_reads();
    let mut stream = ExactSlotStorage::open_stream_at(&home, &slot)
        .await
        .expect("open the CloudKit exact stream");
    let mut parts = Vec::new();
    while let Some(part) = futures_util::StreamExt::next(&mut stream).await {
        parts.push(part.expect("CloudKit body part").to_vec());
    }

    assert_eq!(
        parts.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![CHUNK_SIZE, 1024]
    );
    assert_eq!(parts.concat(), data);
    assert_eq!(
        ops.versioned_reads(),
        vec![
            "audio/streamed-track".to_string(),
            "audio/streamed-track.exact-part0".to_string(),
            "audio/streamed-track.exact-part1".to_string(),
        ],
    );
}

/// A part the provider can no longer serve ends the stream with that error.
/// The parts after a gap are not this object's bytes, so none follow.
#[tokio::test]
async fn a_failed_cloudkit_part_read_ends_the_stream_with_its_error() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("audio/broken-track");
    let data: Vec<u8> = (0..CHUNK_SIZE + 1024)
        .map(|value| (value % 251) as u8)
        .collect();
    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
        .await
        .unwrap();
    ops.delete_record(&CloudKitScope::Private, "audio/broken-track.exact-part1")
        .expect("drop the second part record");

    let mut stream = ExactSlotStorage::open_stream_at(&home, &slot)
        .await
        .expect("open the CloudKit exact stream");
    let first = futures_util::StreamExt::next(&mut stream)
        .await
        .expect("the first part is served")
        .expect("the first part succeeds");
    assert_eq!(first.len(), CHUNK_SIZE);
    let error = futures_util::StreamExt::next(&mut stream)
        .await
        .expect("the missing part is reported")
        .expect_err("a missing part is not a clean end");
    assert!(matches!(error, CloudHomeError::NotFound(_)), "{error}");
    assert!(
        futures_util::StreamExt::next(&mut stream).await.is_none(),
        "nothing follows a failed part"
    );
}

#[tokio::test]
async fn exact_ranged_read_fetches_only_the_parts_it_covers() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("audio/ranged-track");
    // Four parts: three full chunks and a short tail.
    let data: Vec<u8> = (0..3 * CHUNK_SIZE + 1024)
        .map(|value| (value % 251) as u8)
        .collect();
    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
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
    crate::cloud::create_exact_bytes(&home, &slot, b"first", &no_progress())
        .await
        .unwrap();

    let collision = crate::cloud::create_exact_bytes(&home, &slot, b"second", &no_progress())
        .await
        .expect_err("an immutable record must never overwrite an existing key");
    assert!(matches!(collision, CloudHomeError::SlotCollision(key) if key == "copies/bounded"));
    assert_eq!(
        ExactSlotStorage::read_at(&home, &slot).await.unwrap(),
        b"first"
    );

    ops.seed_record("copies/bounded", b"replacement".to_vec());
    let changed = ExactSlotStorage::read_at(&home, &slot)
        .await
        .expect_err("an exact read must reject a replaced manifest");
    assert!(changed.to_string().contains("invalid manifest"));
}

#[tokio::test]
async fn versioned_record_creation_and_replacement_operate_on_the_direct_body() {
    let (home, _) = make_cloud_home_with_ops();
    let slot = exact_slot("publication/current");
    let object = coven_protocol::objects::ExactObjectRef::new(
        slot.clone(),
        5,
        coven_protocol::store_commit::ObjectHash::digest(b"first"),
    );
    let upload = ExactUpload::from_bytes(&object, b"first").expect("build direct record upload");

    ExactSlotStorage::create_versioned_at(&home, &upload, &UploadControl::running(no_progress()))
        .await
        .expect("create direct versioned record");
    let first = ExactSlotStorage::read_versioned_at(&home, &slot)
        .await
        .expect("read initial direct record");
    assert_eq!(first.bytes, b"first");

    let replaced =
        ExactSlotStorage::replace_at_if_version(&home, &slot, &first.version, b"second".to_vec())
            .await
            .expect("replace direct record");
    assert!(matches!(replaced, ConditionalWriteOutcome::Replaced(_)));
    let stale =
        ExactSlotStorage::replace_at_if_version(&home, &slot, &first.version, b"third".to_vec())
            .await
            .expect("settle stale direct record revision");
    assert_eq!(stale, ConditionalWriteOutcome::VersionChanged);
    assert_eq!(
        ExactSlotStorage::read_versioned_at(&home, &slot)
            .await
            .expect("read replaced direct record")
            .bytes,
        b"second"
    );
}

#[tokio::test]
async fn exact_multipart_stages_one_bounded_part_at_a_time_and_manifest_last() {
    let (home, ops) = make_cloud_home_with_ops();
    let data = vec![7u8; CHUNK_SIZE + 13];
    let slot = exact_slot("copies/chunked");

    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
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

    assert_eq!(
        decode_exact_manifest(&ops.stored_record("copies/chunked")).unwrap(),
        ExactManifest {
            part_count: 2,
            total_len: data.len(),
            stored_hash: coven_protocol::store_commit::ObjectHash::digest(&data),
        }
    );
}

#[tokio::test]
async fn lost_atomic_commit_response_is_settled_by_manifest() {
    let (home, ops) = make_cloud_home_with_ops();
    ops.lose_commit_response();
    let data = vec![2u8; CHUNK_SIZE + 1];
    let slot = exact_slot("copies/ambiguous");

    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
        .await
        .expect("the committed manifest settles the create");

    assert_eq!(ExactSlotStorage::read_at(&home, &slot).await.unwrap(), data);
}

#[tokio::test]
async fn concurrent_immutable_creates_have_one_winner() {
    let (home, _) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/create-race");
    let left_progress = no_progress();
    let right_progress = no_progress();

    let (left, right) = tokio::join!(
        crate::cloud::create_exact_bytes(&home, &slot, b"left", &left_progress),
        crate::cloud::create_exact_bytes(&home, &slot, b"right", &right_progress),
    );

    assert!(matches!(
        (&left, &right),
        (
            Ok(crate::cloud::ExactCreateOutcome::Created),
            Err(CloudHomeError::SlotCollision(_)),
        ) | (
            Err(CloudHomeError::SlotCollision(_)),
            Ok(crate::cloud::ExactCreateOutcome::Created),
        )
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

    crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
        .await
        .expect("the committed manifest verifies the exact object");

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

    let error = crate::cloud::create_exact_bytes(&home, &slot, &data, &no_progress())
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
    ops.seed_record(&second_part, b"existing".to_vec());
    let slot = exact_slot("copies/collision");
    let collision_bytes = vec![3u8; CHUNK_SIZE + 1];
    let error = crate::cloud::create_exact_bytes(&home, &slot, &collision_bytes, &no_progress())
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

    let bytes = vec![4u8; CHUNK_SIZE + 1];
    let error = crate::cloud::create_exact_bytes(&home, &slot, &bytes, &no_progress())
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
fn cancellation_discard_failure_does_not_terminate_the_process() {
    const CHILD: &str = "COVEN_CLOUDKIT_CANCEL_DISCARD_ABORT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ops = Arc::new(MockCloudKitOps::new());
            let home = CloudKitCloudHome::new_private(
                ops.clone(),
                coven_foundation::config::ExactUploadVerification::MetadataHash,
            );
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
        return;
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("cancellation_discard_failure_does_not_terminate_the_process")
        .arg("--nocapture")
        .env(CHILD, "1")
        .status()
        .expect("run CloudKit cancellation sabotage subprocess");
    assert!(status.success(), "cancellation subprocess terminated");
}

#[tokio::test]
async fn exact_delete_removes_the_manifest_and_every_part() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/delete");
    let bytes = vec![5u8; CHUNK_SIZE + 1];
    crate::cloud::create_exact_bytes(&home, &slot, &bytes, &no_progress())
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

/// Proving a slot absent must work on whatever kind of record occupies it. A
/// bounded versioned record has no manifest and no parts, so a deletion that
/// first opened the slot as an exact object would refuse to clean up a record
/// that is plainly there.
#[tokio::test]
async fn a_versioned_record_is_deleted_and_verified_absent() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("publication/retired");
    let bytes = b"current publication".to_vec();
    let object = coven_protocol::objects::ExactObjectRef::new(
        slot.clone(),
        bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&bytes),
    );
    ExactSlotStorage::create_versioned_at(
        &home,
        &ExactUpload::from_bytes(&object, &bytes).expect("versioned upload"),
        &UploadControl::running(no_progress()),
    )
    .await
    .expect("create the versioned record");

    ExactSlotStorage::delete_and_verify_absent(&home, &slot)
        .await
        .expect("a versioned record is deleted and proven absent");

    assert!(!ops
        .record_exists(&CloudKitScope::Private, "publication/retired")
        .unwrap());
}

/// The other shape a slot can hold, through the same method: a manifest and
/// every part it names go in one deletion, and the base record answers whether
/// the slot is empty afterwards.
#[tokio::test]
async fn a_multi_part_exact_object_is_deleted_and_verified_absent() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("copies/retired");
    crate::cloud::create_exact_bytes(&home, &slot, &vec![9u8; CHUNK_SIZE + 1], &no_progress())
        .await
        .expect("create the exact object");

    ExactSlotStorage::delete_and_verify_absent(&home, &slot)
        .await
        .expect("an exact object is deleted and proven absent");

    for key in [
        "copies/retired".to_string(),
        exact_part_key("copies/retired", 0),
        exact_part_key("copies/retired", 1),
    ] {
        assert!(!ops.record_exists(&CloudKitScope::Private, &key).unwrap());
    }
}

/// An empty slot is already absent, so proving it so asks nothing of the
/// provider beyond looking.
#[tokio::test]
async fn an_empty_slot_is_already_absent() {
    let (home, ops) = make_cloud_home_with_ops();

    ExactSlotStorage::delete_and_verify_absent(&home, &exact_slot("publication/never-written"))
        .await
        .expect("an empty slot needs no deletion");

    assert!(!ops
        .calls()
        .iter()
        .any(|call| matches!(call, MockCall::Delete(_) | MockCall::DeleteVersions(_))));
}

/// A provider that reports a successful deletion and keeps the record leaves
/// the slot occupied. The caller asked for proof of absence, so this fails
/// rather than reporting the deletion it was told about.
#[tokio::test]
async fn a_record_surviving_its_deletion_fails_the_absence_proof() {
    let (home, ops) = make_cloud_home_with_ops();
    let slot = exact_slot("publication/stubborn");
    let bytes = b"will not go".to_vec();
    let object = coven_protocol::objects::ExactObjectRef::new(
        slot.clone(),
        bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&bytes),
    );
    ExactSlotStorage::create_versioned_at(
        &home,
        &ExactUpload::from_bytes(&object, &bytes).expect("versioned upload"),
        &UploadControl::running(no_progress()),
    )
    .await
    .expect("create the versioned record");
    ops.retain_on_delete("publication/stubborn");

    let error = ExactSlotStorage::delete_and_verify_absent(&home, &slot)
        .await
        .expect_err("a record that survives deletion must not pass as absent");

    assert!(
        error.to_string().contains("publication/stubborn"),
        "{error}"
    );
    assert!(ops
        .record_exists(&CloudKitScope::Private, "publication/stubborn")
        .unwrap());
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

/// The part-name rule a listing filters on, stated from both sides: a record
/// this spelling produces is a part, and a record that merely looks like one
/// is not.
#[test]
fn exact_part_keys_are_recognized_by_their_own_spelling() {
    assert!(is_exact_part_key(&exact_part_key("file.bin", 0)));
    assert!(is_exact_part_key(&exact_part_key("file.bin", 123)));
    assert!(!is_exact_part_key("file.bin"));
    assert!(!is_exact_part_key("file.bin.exact-part"));
    assert!(!is_exact_part_key("file.bin.exact-part1x"));
    assert!(!is_exact_part_key(".exact-part0"));
}
