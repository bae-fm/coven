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

fn recording(location: StorageConfig, state: SessionState, part_size: usize) -> UploadSession {
    UploadSession {
        location,
        path: ObjectPath::device_log(
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        ),
        total: 16 * 1024 * 1024 + 1,
        confirmed: 0,
        part_size,
        state,
    }
}
fn s3_recording() -> UploadSession {
    recording(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            endpoint: None,
            prefix: "store".into(),
        },
        SessionState::S3 {
            id: SecretText::new("id".into()),
            token: SecretText::new("token".into()),
            parts: vec![],
        },
        8 * 1024 * 1024,
    )
}
fn assert_invalid_recording(session: &UploadSession) {
    let bytes = serde_json::to_vec(session).unwrap();
    assert!(
        UploadSession::decode(&bytes).is_err(),
        "accepted invalid recording"
    );
    assert!(session.encode().is_err(), "encoded invalid recording");
    assert!(
        session.check(&session.location).is_err(),
        "accepted invalid session before request"
    );
    assert!(
        serde_json::from_slice::<UploadSession>(&bytes).is_err(),
        "deserialized invalid recording"
    );
}

#[test]
fn direct_deserialization_cannot_bypass_session_validation() {
    let mut session = s3_recording();
    session.path = ObjectPath::positions(coven_foundation::id_source::DeviceId(31));
    let bytes = serde_json::to_vec(&session).unwrap();
    assert!(serde_json::from_slice::<UploadSession>(&bytes).is_err());
}
#[test]
fn session_states_must_match_their_provider_and_have_nonempty_ids() {
    let cases = [
        (
            StorageConfig::GoogleDrive {
                folder_id: "folder".into(),
            },
            SessionState::GoogleDrive {
                url: SecretText::new("https://www.googleapis.com/upload?upload_id=1".into()),
                file_id: SecretText::new("file".into()),
            },
            8 * 1024 * 1024,
        ),
        (
            StorageConfig::Dropbox {
                namespace_id: "namespace".into(),
            },
            SessionState::Dropbox {
                id: SecretText::new("session".into()),
            },
            8 * 1024 * 1024,
        ),
        (
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            SessionState::OneDrive {
                url: SecretText::new("https://upload.onedrive.com/session".into()),
            },
            24 * 320 * 1024,
        ),
        (
            StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            SessionState::CloudKit {
                id: SecretText::new("session".into()),
            },
            1024,
        ),
    ];
    for (location, state, size) in cases {
        let valid = recording(location, state, size);
        UploadSession::decode(valid.encode().unwrap().as_bytes()).unwrap();
        let mut invalid = valid.clone();
        invalid.location = s3_recording().location;
        assert_invalid_recording(&invalid);
        let mut invalid = valid.clone();
        match &mut invalid.state {
            SessionState::GoogleDrive { file_id, .. } => *file_id = SecretText::new(String::new()),
            SessionState::Dropbox { id } | SessionState::CloudKit { id } => {
                *id = SecretText::new(String::new())
            }
            SessionState::OneDrive { url } => *url = SecretText::new(String::new()),
            _ => unreachable!(),
        }
        assert_invalid_recording(&invalid);
        let mut invalid = valid;
        invalid.part_size = 0;
        assert_invalid_recording(&invalid);
    }
    let mut invalid = s3_recording();
    invalid.state = SessionState::VerifyPublished;
    assert_invalid_recording(&invalid);
    for empty_token in [false, true] {
        let mut invalid = s3_recording();
        if let SessionState::S3 { id, token, .. } = &mut invalid.state {
            if empty_token {
                *token = SecretText::new(String::new());
            } else {
                *id = SecretText::new(String::new());
            }
        }
        assert_invalid_recording(&invalid);
    }
}
#[test]
fn s3_recording_parts_agree_with_progress_and_provider_limits() {
    let mut valid = s3_recording();
    valid.confirmed = valid.part_size as u64;
    if let SessionState::S3 { parts, .. } = &mut valid.state {
        parts.push(S3Part {
            number: 1,
            size: valid.confirmed,
            etag: "etag".into(),
        });
    }
    UploadSession::decode(valid.encode().unwrap().as_bytes()).unwrap();
    for mutation in 0..8 {
        let mut invalid = valid.clone();
        let SessionState::S3 { parts, .. } = &mut invalid.state else {
            unreachable!()
        };
        match mutation {
            0 => invalid.confirmed += 1,
            1 => parts[0].number = 2,
            2 => parts[0].size -= 1,
            3 => parts[0].size += 1,
            4 => parts[0].etag.clear(),
            5 => invalid.part_size = 1,
            6 => invalid.total = 10_000 * 5 * 1024u64.pow(3) + 1,
            7 => parts.extend(vec![parts[0].clone(); 10_000]),
            _ => unreachable!(),
        }
        assert_invalid_recording(&invalid);
    }
    valid.total = valid.confirmed + 1;
    valid.confirmed += 1;
    if let SessionState::S3 { parts, .. } = &mut valid.state {
        parts.push(S3Part {
            number: 2,
            size: 1,
            etag: "tail".into(),
        });
    }
    UploadSession::decode(valid.encode().unwrap().as_bytes()).unwrap();
}
#[test]
fn session_urls_and_unknown_state_fields_are_refused() {
    let mut session = recording(
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "folder".into(),
        },
        SessionState::OneDrive {
            url: SecretText::new("https://upload.example/session".into()),
        },
        24 * 320 * 1024,
    );
    for url in [
        "",
        "not a URL",
        "file:///session",
        "https://user:password@upload.example/session",
        "https://upload.example/session#fragment",
    ] {
        session.state = SessionState::OneDrive {
            url: SecretText::new(url.into()),
        };
        assert_invalid_recording(&session);
    }
    let mut value = serde_json::to_value(s3_recording()).unwrap();
    value["state"]["S3"]["unexpected"] = serde_json::json!(true);
    assert!(UploadSession::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn completed_and_verifying_recordings_still_obey_their_provider() {
    let mut session = s3_recording();
    session.state = SessionState::Complete;
    session.confirmed = session.total;
    UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    session.confirmed -= 1;
    assert_invalid_recording(&session);
    session.confirmed = session.total;
    session.part_size = 1;
    assert_invalid_recording(&session);
    session.part_size = 8 * 1024 * 1024;
    session.location = StorageConfig::S3 {
        bucket: String::new(),
        region: "region".into(),
        endpoint: None,
        prefix: "store".into(),
    };
    assert_invalid_recording(&session);

    let mut session = recording(
        StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        SessionState::VerifyPublished,
        8 * 1024 * 1024,
    );
    UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    session.confirmed = session.total;
    assert_invalid_recording(&session);
    session.confirmed = 0;
    session.part_size = 1;
    assert_invalid_recording(&session);
    session.part_size = 8 * 1024 * 1024;
    assert!(matches!(
        session.check(&StorageConfig::Dropbox {
            namespace_id: "another".into()
        }),
        Err(StorageError::SessionMismatch)
    ));
}
