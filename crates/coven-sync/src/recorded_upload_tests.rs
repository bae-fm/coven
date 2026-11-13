use super::*;
use coven_storage::{ObjectPath, ObjectPrefix, StorageConfig, StoredObject};
use serde_json::json;
use std::sync::{Arc, Mutex};

struct RecordingSource {
    events: Arc<Mutex<Vec<&'static str>>>,
    fail_save: Option<usize>,
    saves: usize,
}

impl UploadSource for RecordingSource {
    type Error = StorageError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError> {
        self.events.lock().unwrap().push("read");
        Ok(b"data"[offset as usize..offset as usize + length].to_vec())
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), StorageError> {
        self.saves += 1;
        if self.fail_save == Some(self.saves) {
            return Err(StorageFailure::File.into());
        }
        // Exercise the durable representation, including its planned state.
        let session = UploadSession::decode(session.encode()?.as_bytes())?;
        self.events.lock().unwrap().push(if session.needs_initiation() {
            "save planned"
        } else {
            "save native"
        });
        Ok(())
    }
    fn keep_going(&self) -> bool {
        true
    }
}

struct PlannedStorage(Arc<Mutex<Vec<&'static str>>>);

fn with_state(session: &UploadSession, state: serde_json::Value, confirmed: u64) -> UploadSession {
    let mut value: serde_json::Value =
        serde_json::from_slice(session.encode().unwrap().as_bytes()).unwrap();
    value["state"] = state;
    value["confirmed"] = json!(confirmed);
    UploadSession::decode(&serde_json::to_vec(&value).unwrap()).unwrap()
}

#[async_trait::async_trait]
impl Storage for PlannedStorage {
    fn config(&self) -> StorageConfig {
        StorageConfig::OneDrive {
            account_label: None,
            drive_id: "drive".into(),
            folder_id: "folder".into(),
            folder_label: None,
        }
    }
    fn single_request_limit(&self) -> u64 {
        250_000_000
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.0.lock().unwrap().push("plan");
        UploadSession::decode(&serde_json::to_vec(&json!({
            "device": coven_storage::test_utils::DEVICE,
            "location": self.config(), "path": path, "total": total, "confirmed": 0,
            "part_size": 24 * 320 * 1024,
            "state": {"OneDrive": {"temporary": "00000000-0000-0000-0000-000000000001", "upload": {"Planned": {}}}}
        })).unwrap())
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let mut events = self.0.lock().unwrap();
        assert_eq!(events.last(), Some(&"save planned"));
        events.push("initiate");
        *session = with_state(
            session,
            json!({"OneDrive": {
                "temporary": "00000000-0000-0000-0000-000000000001",
                "upload": {"Receiving": {"url": "https://upload.example/session"}}
            }}),
            0,
        );
        Ok(())
    }
    fn admit_part<'a>(
        &self,
        session: &mut UploadSession,
        bytes: &'a [u8],
    ) -> Result<coven_storage::PartPermit<'a>, StorageError> {
        assert_eq!(bytes, b"data");
        coven_storage::test_utils::admitted_part(session, bytes)
    }
    async fn send_independent_part(
        &self,
        _: &UploadSession,
        _: u64,
        _: &[u8],
        _: &coven_storage::ByteProgress,
    ) -> Result<coven_storage::ConfirmedPart, StorageError> {
        unreachable!("a OneDrive session's parts go in order")
    }
    async fn send_part(
        &self,
        _: coven_storage::PartPermit<'_>,
        session: &mut UploadSession,
        _: &coven_storage::ByteProgress,
    ) -> Result<(), StorageError> {
        let mut events = self.0.lock().unwrap();
        assert!(events.contains(&"save native"), "native capability was not saved before sending");
        events.push("part");
        *session = with_state(
            session,
            json!({"OneDrive": {
                "temporary": "00000000-0000-0000-0000-000000000001",
                "upload": {"Uploaded": {"item": "temporary"}}
            }}),
            4,
        );
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.0.lock().unwrap().push("finish");
        *session = with_state(session, json!("Complete"), 4);
        Ok(())
    }
    async fn abort_upload(&self, _: &UploadSession) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn end_settled_claims(
        &self,
        _: coven_foundation::id_source::StoreId,
        _: u64,
    ) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn end_device_uploads(
        &self,
        _: coven_foundation::id_source::StoreId,
        _: coven_foundation::id_source::DeviceId,
    ) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn create_reporting(
        &self,
        _: &ObjectPath,
        _: &[u8],
        _: &coven_storage::ByteProgress,
    ) -> Result<Option<StoredObject>, StorageError> {
        unreachable!()
    }
    async fn replace(&self, _: &StoredObject, _: &[u8]) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn status(&self, _: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        unreachable!()
    }
    async fn stream_object_reporting(
        &self,
        _: &ObjectPath,
        _: Option<coven_storage::ByteRange>,
        _: &coven_storage::ByteProgress,
    ) -> Result<coven_storage::ObjectStream<'_>, StorageError> {
        unreachable!()
    }
    async fn list(&self, _: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        unreachable!()
    }
    async fn delete(&self, _: &ObjectPath) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn grant_access(&self, _: &str) -> Result<coven_storage::AccessGrant, StorageError> {
        unreachable!()
    }
    async fn revoke_access(
        &self,
        _: &str,
    ) -> Result<Vec<coven_storage::RetainedAccess>, StorageError> {
        unreachable!()
    }
}

#[tokio::test]
async fn identity_and_native_capability_are_saved_before_their_first_use() {
    for fail_save in [None, Some(1), Some(2)] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let storage = PlannedStorage(events.clone());
        let mut source = RecordingSource { events: events.clone(), fail_save, saves: 0 };
        let path = ObjectPath::slot(coven_storage::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
        let result = upload(&storage, &path, 4, None, &mut source).await;
        let expected = match fail_save {
            None => {
                assert!(matches!(result, Ok(Uploaded::Stored(None))));
                vec![
                    "plan",
                    "save planned",
                    "initiate",
                    "save native",
                    "read",
                    "part",
                    "save native",
                    "finish",
                    "save native",
                ]
            }
            Some(1) => {
                assert_eq!(result.unwrap_err().failure(), StorageFailure::File);
                vec!["plan"]
            }
            Some(2) => {
                assert_eq!(result.unwrap_err().failure(), StorageFailure::File);
                vec!["plan", "save planned", "initiate"]
            }
            _ => unreachable!(),
        };
        assert_eq!(*events.lock().unwrap(), expected);
    }
}

/// A source whose saves keep the recording and close `lever` once the plan
/// is saved, as another consumer arming a provider cooldown, or sync
/// falling behind, would between the plan and its first part.
struct ClosingSource {
    saved: Vec<coven_crypto::SecretBytes>,
    lever: Arc<std::sync::atomic::AtomicBool>,
}

impl UploadSource for ClosingSource {
    type Error = StorageError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError> {
        Ok(b"data"[offset as usize..offset as usize + length].to_vec())
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), StorageError> {
        self.saved.push(session.encode()?);
        self.lever.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn keep_going(&self) -> bool {
        true
    }
}

/// A provider cooldown another consumer armed.
struct Cooling(Arc<std::sync::atomic::AtomicBool>);
#[async_trait::async_trait]
impl coven_storage::Cooldown for Cooling {
    fn remaining(&self) -> Option<std::time::Duration> {
        self.0
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(std::time::Duration::from_secs(30))
    }
    async fn record(&self, _: std::time::Duration) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Sync falling behind: nothing may be published until a catch-up.
struct CatchingUp(Arc<std::sync::atomic::AtomicBool>);
impl coven_storage::PublicationGate for CatchingUp {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}

/// A claim's plan is saved, and then a provider cooldown, or a catch-up,
/// refuses its first part before any request: the saved plan stays unmarked,
/// so the next attempt goes on from it at the shared slot, and publishes
/// (§17.9, Appendix E.10).
#[tokio::test]
async fn a_part_refused_before_any_request_leaves_the_saved_plan_unmarked() {
    for refusal in [StorageFailure::RateLimited, StorageFailure::CatchUpRequired] {
        let lever = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let raw = coven_storage::test_utils::MemoryStorage::builder().build().unwrap();
        let storage = match refusal {
            StorageFailure::RateLimited => raw.with_cooldown(Arc::new(Cooling(lever.clone()))),
            _ => raw.with_publication_gate(Arc::new(CatchingUp(lever.clone()))),
        };
        let path = ObjectPath::slot(coven_storage::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
        let mut source = ClosingSource { saved: Vec::new(), lever: lever.clone() };
        let error = upload(&storage, &path, 4, None, &mut source).await.unwrap_err();
        assert_eq!(error.failure(), refusal);
        assert_eq!(source.saved.len(), 1, "only the plan was saved");
        assert!(storage.unfinished_uploads().await.is_empty(), "nothing was started");
        let plan = UploadSession::decode(source.saved[0].as_bytes()).unwrap();
        let mut source =
            RecordingSource { events: Arc::new(Mutex::new(Vec::new())), fail_save: None, saves: 0 };
        lever.store(false, std::sync::atomic::Ordering::SeqCst);
        let stored = upload(&storage, &path, 4, Some(plan), &mut source).await.unwrap();
        assert!(matches!(stored, Uploaded::Stored(None)));
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
    }
}

/// Keeps every session the upload saves.
#[derive(Default)]
struct SavingSource {
    saved: Vec<coven_crypto::SecretBytes>,
}

impl UploadSource for SavingSource {
    type Error = StorageError;
    async fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError> {
        Ok(b"data"[offset as usize..offset as usize + length].to_vec())
    }
    async fn save(&mut self, session: UploadSession) -> Result<(), StorageError> {
        self.saved.push(session.encode()?);
        Ok(())
    }
    fn keep_going(&self) -> bool {
        true
    }
}

/// A claim's first part is admitted, so its plan is saved marked, and then
/// sync falls behind while the start waits to go out: the publication gate,
/// checked again as the request goes, refuses it having sent nothing. The
/// plan is saved unmarked again before the failure returns, so the next
/// attempt goes on from it at the shared slot (§17.9, Appendix E.10).
#[tokio::test]
async fn a_start_refused_as_it_goes_out_saves_its_plan_unmarked() {
    let behind = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let storage = coven_storage::test_utils::MemoryStorage::builder()
        .build()
        .unwrap()
        .with_publication_gate(Arc::new(CatchingUp(behind.clone())));
    let path = ObjectPath::slot(coven_storage::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_request_of(
            coven_storage::test_utils::MemoryRequest::Other("initiate_upload"),
            started,
            held,
        )
        .await;
    let mut source = SavingSource::default();
    let falling_behind = async {
        waiting.await.unwrap();
        behind.store(true, std::sync::atomic::Ordering::SeqCst);
        resume.send(()).unwrap();
    };
    let (refused, ()) = tokio::join!(upload(&storage, &path, 4, None, &mut source), falling_behind);
    assert_eq!(refused.unwrap_err().failure(), StorageFailure::CatchUpRequired);
    assert!(storage.unfinished_uploads().await.is_empty(), "nothing was started");
    assert_eq!(source.saved.len(), 3, "the plan, marked, then unmarked again");
    let plan = UploadSession::decode(source.saved[2].as_bytes()).unwrap();

    behind.store(false, std::sync::atomic::Ordering::SeqCst);
    let mut source =
        RecordingSource { events: Arc::new(Mutex::new(Vec::new())), fail_save: None, saves: 0 };
    let stored = upload(&storage, &path, 4, Some(plan), &mut source).await.unwrap();
    assert!(matches!(stored, Uploaded::Stored(None)));
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}
