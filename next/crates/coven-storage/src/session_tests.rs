use super::*;
use coven_crypto::SecretText;

#[test]
fn upload_sessions_encode_without_capacity_growth() {
    let session = UploadSession {
        location: StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        path: ObjectPath::store_log(
            coven_foundation::id_source::DeviceId(1),
            std::num::NonZeroU64::MIN,
        ),
        total: 42,
        confirmed: 0,
        part_size: 8 * 1024 * 1024,
        state: SessionState::Dropbox {
            id: SecretText::new("session\"\\\n\0雪".repeat(200)),
        },
    };
    let encoded = session.encode().unwrap();
    assert_eq!(encoded.capacity(), encoded.as_bytes().len());
    let decoded = UploadSession::decode(encoded.as_bytes()).unwrap();
    assert_eq!(decoded.path(), session.path());
    let SessionState::Dropbox { id } = decoded.state else {
        panic!("wrong session kind")
    };
    assert_eq!(id.as_str(), "session\"\\\n\0雪".repeat(200));
}

#[test]
fn recorded_upload_refuses_positions_even_when_complete() {
    for state in [
        SessionState::Dropbox {
            id: SecretText::new("upload".into()),
        },
        SessionState::Complete,
    ] {
        let session = UploadSession {
            location: StorageConfig::Dropbox {
                namespace_id: "namespace".into(),
            },
            path: ObjectPath::positions(coven_foundation::id_source::DeviceId(1)),
            total: 1,
            confirmed: 1,
            part_size: 8 * 1024 * 1024,
            state,
        };
        let bytes = serde_json::to_vec(&session).unwrap();
        assert!(matches!(
            UploadSession::decode(&bytes),
            Err(StorageError::InvalidPath)
        ));
    }
}
