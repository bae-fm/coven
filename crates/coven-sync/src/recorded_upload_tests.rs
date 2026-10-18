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
        self.events
            .lock()
            .unwrap()
            .push(if session.needs_initiation() {
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
            "location": self.config(), "target": {"Create": path}, "total": total, "confirmed": 0,
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
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let mut events = self.0.lock().unwrap();
        assert!(
            events.contains(&"save native"),
            "native capability was not saved before sending"
        );
        events.push("part");
        assert_eq!(bytes, b"data");
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
    async fn create(&self, _: &ObjectPath, _: &[u8]) -> Result<Option<StoredObject>, StorageError> {
        unreachable!()
    }
    async fn replace(&self, _: &StoredObject, _: &[u8]) -> Result<(), StorageError> {
        unreachable!()
    }
    async fn begin_replace(&self, _: &StoredObject, _: u64) -> Result<UploadSession, StorageError> {
        unreachable!()
    }
    async fn status(&self, _: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        unreachable!()
    }
    async fn stream_object(
        &self,
        _: &ObjectPath,
        _: Option<coven_storage::ByteRange>,
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
        let mut source = RecordingSource {
            events: events.clone(),
            fail_save,
            saves: 0,
        };
        let path = ObjectPath::slot(
            coven_storage::test_utils::STORE_ID,
            std::num::NonZeroU64::MIN,
        );
        let result = upload(&storage, &UploadTarget::Create(path), 4, None, &mut source).await;
        let expected = match fail_save {
            None => {
                assert!(matches!(result, Ok(Uploaded::Stored)));
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
