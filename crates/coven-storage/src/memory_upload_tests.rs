use super::*;
use serde_json::{json, Value};

async fn floor_bytes(storage: &MemoryStorage, path: &ObjectPath) -> Option<Vec<u8>> {
    match storage.status(path).await.unwrap() {
        None => None,
        Some(_) => Some(storage.read(path).await.unwrap()),
    }
}

async fn execute(initial: Option<Vec<u8>>, total: usize, winner: usize) -> (Value, Value) {
    let storage = MemoryStorage::builder()
        .transfer_limits(4, 4)
        .build()
        .unwrap();
    let path = ObjectPath::floor(STORE_ID);
    if let Some(bytes) = &initial {
        storage.create(&path, bytes).await.unwrap();
    }
    let target = match storage.status(&path).await.unwrap() {
        None => UploadTarget::Create(path.clone()),
        Some(previous) => UploadTarget::Replace(previous),
    };
    let mut sessions = Vec::new();
    for _ in 0..2 {
        sessions.push(match &target {
            UploadTarget::Create(path) => storage.begin_upload(path, total as u64).await.unwrap(),
            UploadTarget::Replace(previous) => {
                storage.begin_replace(previous, total as u64).await.unwrap()
            }
        });
    }
    let mut ops = Vec::new();
    // All incomplete completions must preserve the previous floor. Staging
    // both contenders first also prevents a begin-time check from hiding CAS.
    for index in [0, 1] {
        ops.push(json!({"op":"publish", "session":index}));
        for (part_index, part) in vec![index as u8 + 1; total].chunks(4).enumerate() {
            ops.push(json!({"op":"part", "session":index, "bytes":part}));
            if (part_index + 1) * 4 < total {
                ops.push(json!({"op":"publish", "session":index}));
            }
        }
    }
    ops.push(json!({"op":"publish", "session":winner}));
    ops.push(json!({"op":"publish", "session":1-winner}));
    let input = json!({"initial":initial, "totals":[total,total], "ops":ops});
    let mut results = Vec::new();
    for op in &ops {
        let index = op["session"].as_u64().unwrap() as usize;
        let session = &mut sessions[index];
        *session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(session).await.unwrap();
        assert_eq!(session.target(), &target);
        let result = if op["op"] == "part" {
            let bytes: Vec<u8> = serde_json::from_value(op["bytes"].clone()).unwrap();
            storage.upload_part(session, &bytes).await.unwrap();
            "received"
        } else {
            match storage.finish_upload(session).await {
                Ok(()) => "stored",
                Err(error) if error.failure() == StorageFailure::InvalidPart => "incomplete",
                Err(error)
                    if matches!(
                        error.failure(),
                        StorageFailure::AlreadyExists | StorageFailure::Conflict
                    ) =>
                {
                    "conflict"
                }
                Err(error) => panic!("{error:?}"),
            }
        };
        results.push(json!({"result":result, "bytes":floor_bytes(&storage, &path).await}));
    }
    assert_eq!(
        storage.read(&path).await.unwrap(),
        vec![winner as u8 + 1; total]
    );
    for session in sessions {
        storage.abort_upload(&session).await.unwrap();
    }
    assert!(storage.provider.state.lock().await.uploads.is_empty());
    (input, json!(results))
}

#[tokio::test]
async fn conditional_parts_publish_whole_bytes_once_across_recording_boundaries() {
    for initial in [None, Some(b"old".to_vec())] {
        for total in [1, 4, 5, 8, 9] {
            for winner in [0, 1] {
                execute(initial.clone(), total, winner).await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires COVEN_FLOOR_UPLOAD_LEAN"]
async fn conditional_floor_upload_matches_lean() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let runner = std::env::var("COVEN_FLOOR_UPLOAD_LEAN").expect("floorUploadRunner executable");
    for initial in [None, Some(b"old".to_vec())] {
        for total in [1, 4, 5, 8, 9] {
            for winner in [0, 1] {
                let (input, actual) = execute(initial.clone(), total, winner).await;
                let mut child = Command::new(&runner)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(input.to_string().as_bytes())
                    .unwrap();
                let output = child.wait_with_output().unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let expected: Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(actual, expected, "{input}");
            }
        }
    }
}

#[tokio::test]
async fn a_replacement_restart_keeps_the_original_revision_after_expiry() {
    let storage = MemoryStorage::builder()
        .transfer_limits(4, 4)
        .build()
        .unwrap();
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"old").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let session = storage.begin_replace(&previous, 5).await.unwrap();
    storage
        .set_faults(Faults {
            expire_uploads: true,
            ..Faults::none()
        })
        .await;
    storage.replace(&previous, b"next").await.unwrap();
    let error = storage.restart_upload(&session).await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::Conflict);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
}
