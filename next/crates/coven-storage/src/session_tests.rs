use super::*;
use coven_crypto::SecretText;

#[test]
fn upload_sessions_encode_without_capacity_growth() {
    let session = UploadSession {
        location: StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        path: ObjectPath::positions(coven_foundation::id_source::DeviceId(1)),
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
