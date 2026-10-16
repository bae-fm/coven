use super::*;
use crate::test_utils::STORE_ID;
use coven_crypto::SecretText;

#[test]
fn upload_sessions_encode_without_capacity_growth() {
    let session = UploadSession {
        location: StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "namespace".into(),
            },
        },
        path: ObjectPath::device_log(
            STORE_ID,
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
                location: crate::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
            },
            path: ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(1)),
            total: 1,
            confirmed: 1,
            part_size: 8 * 1024 * 1024,
            state,
        };
        let bytes = serde_json::to_vec(&session).unwrap();
        assert!(matches!(
            UploadSession::decode(&bytes),
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ));
    }
}

fn recording(location: StorageConfig, state: SessionState, part_size: usize) -> UploadSession {
    UploadSession {
        location,
        path: ObjectPath::device_log(
            STORE_ID,
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
            profile: None,
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
    session.path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    let bytes = serde_json::to_vec(&session).unwrap();
    assert!(serde_json::from_slice::<UploadSession>(&bytes).is_err());
}
#[test]
fn session_states_must_match_their_provider_and_have_nonempty_ids() {
    let cases = [
        (
            StorageConfig::Dropbox {
                location: crate::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
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
            onedrive_state("https://upload.onedrive.com/session"),
            24 * 320 * 1024,
        ),
        (
            StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            SessionState::CloudKit {
                id: SecretText::new(uuid::Uuid::from_u128(1).to_string()),
            },
            CLOUDKIT_PART_SIZE,
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
            SessionState::Dropbox { id } | SessionState::CloudKit { id } => {
                *id = SecretText::new(String::new())
            }
            SessionState::OneDrive {
                upload: OneDriveUpload::Receiving { url },
                ..
            } => *url = SecretText::new(String::new()),
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
fn restarting_s3_session_keeps_only_cleanup_identifiers_and_zero_progress() {
    let mut session = s3_recording();
    session.state = SessionState::S3Restart {
        id: SecretText::new("native-upload".into()),
        token: SecretText::new("token".into()),
    };
    UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    session.confirmed = 1;
    assert_invalid_recording(&session);
    session.confirmed = 0;
    session.location = StorageConfig::Dropbox {
        location: crate::DropboxLocation::SharedFolder {
            namespace_id: "namespace".into(),
        },
    };
    assert_invalid_recording(&session);
    session.location = s3_recording().location;
    for (id, token) in [("", "token"), ("id", "")] {
        session.state = SessionState::S3Restart {
            id: SecretText::new(id.into()),
            token: SecretText::new(token.into()),
        };
        assert_invalid_recording(&session);
    }
}
#[test]
fn session_urls_and_unknown_state_fields_are_refused() {
    let mut session = recording(
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "folder".into(),
        },
        onedrive_state("https://upload.example/session"),
        24 * 320 * 1024,
    );
    for url in [
        "http://localhost/session?token=1",
        "https://upload.example/session?token=1",
    ] {
        session.state = onedrive_state(url);
        UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
    }
    for url in [
        "",
        "not a URL",
        "file:///session",
        "https://user:password@upload.example/session",
        "https://upload.example/session#fragment",
    ] {
        session.state = onedrive_state(url);
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
        profile: None,
        prefix: "store".into(),
    };
    assert_invalid_recording(&session);

    let mut session = recording(
        StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "namespace".into(),
            },
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
        session.check(&StorageConfig::Dropbox { location: crate::DropboxLocation::SharedFolder { namespace_id: "another".into() } }),
        Err(error) if error.failure() == StorageFailure::SessionMismatch
    ));
}

fn onedrive_state(url: &str) -> SessionState {
    SessionState::OneDrive {
        temporary: uuid::Uuid::from_bytes([7; 16]),
        upload: OneDriveUpload::Receiving {
            url: SecretText::new(url.into()),
        },
    }
}

#[test]
fn onedrive_publication_phases_require_their_real_progress_and_capability() {
    let location = StorageConfig::OneDrive {
        drive_id: "drive".into(),
        folder_id: "folder".into(),
    };
    for upload in [
        OneDriveUpload::Uploaded {
            item: "item".into(),
        },
        OneDriveUpload::Copying {
            monitor: SecretText::new("https://monitor.example/copy".into()),
        },
        OneDriveUpload::Published,
        OneDriveUpload::Refused,
    ] {
        let mut session = recording(
            location.clone(),
            SessionState::OneDrive {
                temporary: uuid::Uuid::from_bytes([7; 16]),
                upload,
            },
            ONEDRIVE_PART_SIZE,
        );
        assert_invalid_recording(&session);
        session.confirmed = session.total;
        UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        let SessionState::OneDrive { upload, .. } = &mut session.state else {
            unreachable!()
        };
        match upload {
            OneDriveUpload::Uploaded { item } => item.clear(),
            OneDriveUpload::Copying { monitor } => {
                *monitor = SecretText::new("file:///copy".into())
            }
            OneDriveUpload::Published | OneDriveUpload::Refused => {
                *upload = OneDriveUpload::VerifyPublished
            }
            _ => unreachable!(),
        }
        assert_invalid_recording(&session);
    }
    let mut session = recording(
        location,
        onedrive_state("https://upload.example/session"),
        ONEDRIVE_PART_SIZE,
    );
    session.confirmed = session.total;
    assert_invalid_recording(&session);
    session.confirmed = 0;
    session.state = SessionState::VerifyPublished;
    assert_invalid_recording(&session);
}
