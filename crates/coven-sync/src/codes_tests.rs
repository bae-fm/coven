use super::*;
use coven_crypto::{InviteSecret, SecretBytes, SecretText};
use coven_foundation::id_source::{DeviceId, InviteId, KeyId};
use coven_storage::{RestoreStorage, S3Credentials, StorageConfig, StorageInvitation};

fn location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "region".into(),
        endpoint: Some("https://s3.us-east-1.amazonaws.com".parse().unwrap()),
        profile: None,
        prefix: coven_storage::S3Prefix::parse("store").unwrap(),
    }
}

fn credentials() -> S3Credentials {
    S3Credentials {
        access_key_id: "access".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

fn invite(storage: InviteStorage) -> InviteCode {
    InviteCode {
        store: StoreId(uuid::Uuid::from_u128(1)),
        name: "Household".into(),
        invitee: Some("Dan".into()),
        delivery: InviteDeliveryKind::Sent,
        invite: InviteId(uuid::Uuid::from_u128(2)),
        // The admin's chosen id for the invited person, not the inviter's.
        member: MemberId(uuid::Uuid::from_u128(2)),
        secret: InviteSecret::generate().unwrap(),
        initial_key: KeyId(uuid::Uuid::from_u128(3)),
        inviting_writer: DeviceId(4),
        read_through: 12,
        storage: storage.encode().unwrap(),
    }
}

#[test]
fn restore_metadata_retains_each_location_and_its_sign_in_requirement() {
    for (location, access) in [
        (location(), None),
        (
            StorageConfig::Dropbox {
                account_label: Some("ana@example.com".into()),
                location: coven_storage::DropboxLocation::AppFolder,
            },
            Some(OAuthAccess::Dropbox(coven_storage::DropboxAccess::AppFolder)),
        ),
        (
            StorageConfig::Dropbox {
                account_label: Some("ana@work.example".into()),
                location: coven_storage::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
            },
            Some(OAuthAccess::Dropbox(coven_storage::DropboxAccess::FullDropbox)),
        ),
        (
            StorageConfig::OneDrive {
                account_label: Some("ana@work.example".into()),
                drive_id: "drive".into(),
                folder_id: "household".into(),
            },
            Some(OAuthAccess::OneDrive),
        ),
        (
            StorageConfig::CloudKit {
                container: "iCloud.notes".into(),
                owner: "owner-record".into(),
                zone: "household".into(),
            },
            None,
        ),
    ] {
        let code = RestoreCode {
            store: StoreId(uuid::Uuid::from_u128(1)),
            member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
            member_keys: coven_crypto::MemberKeys::generate().unwrap(),
            storage: RestoreStorage::location_only(location.clone()).encode().unwrap(),
        }
        .to_text()
        .unwrap();
        let CodeInfo::Restore(metadata) = decode_code_info(&code).unwrap() else {
            panic!("restore metadata");
        };
        assert_eq!(metadata.oauth_access, access);
        assert_eq!(metadata.cloud_provider, location.provider());
        assert_eq!(metadata.location, location);
        assert!(!metadata.carries_s3_key);
        let decoded = read_restore_code(&code).unwrap();
        let storage = RestoreStorage::decode(decoded.storage.as_bytes()).unwrap();
        assert_eq!(storage.location, location);
        assert!(storage.s3_key.is_none());
    }
}

#[test]
fn a_restore_code_carrying_the_s3_key_says_so_and_gives_it_back() {
    let code = RestoreCode {
        store: StoreId(uuid::Uuid::from_u128(1)),
        member: MemberId(uuid::Uuid::from_u128(1)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: RestoreStorage { location: location(), s3_key: Some(credentials()) }
            .encode()
            .unwrap(),
    }
    .to_text()
    .unwrap();
    let info = decode_code_info(&code).unwrap();
    assert!(info.metadata().carries_s3_key);
    assert_eq!(info.metadata().location, location());
    assert!(!format!("{info:?}").contains("secret"));
    let decoded = read_restore_code(&code).unwrap();
    let key = RestoreStorage::decode(decoded.storage.as_bytes()).unwrap().s3_key.unwrap();
    assert_eq!(key.access_key_id, "access");
    assert_eq!(key.secret_access_key.as_str(), "secret");
}

#[test]
fn metadata_validates_kind_checksum_and_storage_without_exposing_secrets() {
    let restore = RestoreCode {
        store: StoreId(uuid::Uuid::from_u128(1)),
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: RestoreStorage::location_only(location()).encode().unwrap(),
    };
    let restore_text = restore.to_text().unwrap();
    let invite = invite(InviteStorage::S3 {
        invitation: StorageInvitation::for_account(location()).unwrap(),
        credentials: credentials(),
    });
    let invite_text = invite.to_text().unwrap();
    let decoded = read_invite_code(&invite_text).unwrap();
    assert_eq!(decoded.initial_key, invite.initial_key);
    assert_eq!(decoded.inviting_writer, invite.inviting_writer);
    assert_eq!(decoded.read_through, invite.read_through);
    for (text, kind, member) in [
        (&restore_text, CodeKind::Restore, restore.member),
        (&invite_text, CodeKind::Invite, invite.member),
    ] {
        assert_eq!(decode_code_info(text).unwrap(), {
            let metadata = CodeMetadata {
                store_id: restore.store,
                member,
                cloud_provider: CloudProvider::S3,
                location: location(),
                oauth_access: None,
                // The S3 invite carries the member's key; this restore code doesn't.
                carries_s3_key: kind == CodeKind::Invite,
            };
            match kind {
                CodeKind::Restore => CodeInfo::Restore(metadata),
                CodeKind::Invite => CodeInfo::Invite(InviteInfo {
                    metadata,
                    store_name: "Household".into(),
                    invitee: Some("Dan".into()),
                    delivery: InviteDeliveryKind::Sent,
                }),
            }
        });
        assert!(!format!("{:?}", decode_code_info(text).unwrap()).contains("secret"));
        let truncated = &text[..text.len() - 1];
        assert!(matches!(decode_code_info(truncated), Err(CodeError::Invalid)));
    }
    assert!(matches!(
        read_invite_code(&restore_text),
        Err(CodeError::WrongKind { expected: CodeKind::Invite, actual: CodeKind::Restore })
    ));
    assert!(matches!(
        read_restore_code(&invite_text),
        Err(CodeError::WrongKind { expected: CodeKind::Restore, actual: CodeKind::Invite })
    ));
    let mut invalid = restore;
    invalid.storage = SecretBytes::new(b"invalid payload".to_vec());
    assert!(matches!(decode_code_info(&invalid.to_text().unwrap()), Err(CodeError::Invalid)));
    // An invite's storage part in a restore code: the key where a restore
    // code says whether one follows.
    invalid.storage = SecretBytes::new(invite.storage.as_bytes().to_vec());
    assert!(matches!(read_restore_code(&invalid.to_text().unwrap()), Err(CodeError::Invalid)));
    // An S3 invite whose key is missing.
    let mut invalid = invite;
    invalid.storage = RestoreStorage::location_only(location()).encode().unwrap();
    assert!(matches!(decode_code_info(&invalid.to_text().unwrap()), Err(CodeError::Invalid)));
}

#[test]
fn invite_metadata_exposes_locations_without_provider_admission_secrets() {
    use serde_json::json;
    for (location, acceptance) in [
        (
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "household".into(),
                account_label: Some("ana@example.com".into()),
            },
            json!({"OneDriveShare":{"token":"provider-admission-secret"}}),
        ),
        (
            StorageConfig::CloudKit {
                container: "iCloud.notes".into(),
                owner: "owner-record".into(),
                zone: "household".into(),
            },
            json!({"CloudKitShare":{"url":"https://www.icloud.com/share/provider-admission-secret"}}),
        ),
    ] {
        let invitation = StorageInvitation::decode(
            &serde_json::to_vec(&json!({"location":location,"acceptance":acceptance})).unwrap(),
        )
        .unwrap();
        let code = invite(InviteStorage::Account(invitation));
        let info = decode_code_info(&code.to_text().unwrap()).unwrap();
        assert_eq!(info.metadata().location, location);
        assert!(!info.metadata().carries_s3_key);
        assert!(!format!("{info:?}").contains("provider-admission-secret"));
    }
}

/// Typical stores of each provider, as setup records them, with the key or
/// share an invite for each carries.
fn typical() -> Vec<(&'static str, StorageConfig, InviteStorage)> {
    let s3_key = || S3Credentials {
        access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
        secret_access_key: SecretText::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into()),
    };
    let s3 = |location: StorageConfig, key: S3Credentials| {
        let invite = InviteStorage::S3 {
            invitation: StorageInvitation::for_account(location.clone()).unwrap(),
            credentials: key,
        };
        (location, invite)
    };
    let (aws, aws_invite) = s3(
        StorageConfig::S3 {
            bucket: "ana-music-library".into(),
            region: "us-east-1".into(),
            endpoint: None,
            profile: None,
            prefix: coven_storage::S3Prefix::parse("bae").unwrap(),
        },
        s3_key(),
    );
    let (detected, detected_invite) = s3(
        StorageConfig::S3 {
            bucket: "ana-music-library".into(),
            region: "us-west-004".into(),
            endpoint: Some("https://s3.us-west-004.backblazeb2.com".parse().unwrap()),
            profile: Some(coven_storage::S3Profile::Custom(coven_storage::S3Settings {
                signing: coven_storage::S3Signing::AwsSigV4,
                single_put: coven_storage::S3SinglePut::IfNoneMatch,
                copy_publication: coven_storage::S3CopyPublication::Never,
                multipart_timestamp: coven_storage::S3MultipartTimestamp::Completion,
                checksums: coven_storage::S3Checksums::UnstatedIgnored,
                listing: coven_storage::S3Listing::V2,
                revision: coven_storage::S3Revision::ETag,
                response_headers: coven_storage::S3ResponseHeaders::S3,
                request_checksums: false,
            })),
            prefix: coven_storage::S3Prefix::default(),
        },
        S3Credentials {
            access_key_id: "0040123456789ab0000000001".into(),
            secret_access_key: SecretText::new("K004abcdefghijklmnopqrstuvwxyz01234".into()),
        },
    );
    let (gcs, gcs_invite) = s3(
        StorageConfig::S3 {
            bucket: "ana-music-library".into(),
            region: "auto".into(),
            endpoint: Some("https://storage.googleapis.com".parse().unwrap()),
            profile: None,
            prefix: coven_storage::S3Prefix::parse("bae").unwrap(),
        },
        S3Credentials {
            access_key_id: "GOOG1EABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRS".into(),
            secret_access_key: SecretText::new("bGoa+V7g/yqDXvKRqq+JTFn4uQZbPiQJo4pf9RzJ".into()),
        },
    );
    let dropbox = StorageConfig::Dropbox {
        location: coven_storage::DropboxLocation::SharedFolder {
            namespace_id: "12345678901".into(),
        },
        account_label: Some("ana@example.com".into()),
    };
    let onedrive = StorageConfig::OneDrive {
        drive_id: "0123456789abcdef".into(),
        folder_id: "0123456789ABCDEF!1234".into(),
        account_label: Some("ana@example.com".into()),
    };
    let icloud = StorageConfig::CloudKit {
        container: "iCloud.fm.bae.desktop".into(),
        owner: "_0123456789abcdef0123456789abcdef".into(),
        zone: "coven-5d0c8e2a-7b1f-4c3e-9a64-0f2d8b71c3e5".into(),
    };
    let account = |location: &StorageConfig, acceptance: serde_json::Value| {
        InviteStorage::Account(
            StorageInvitation::decode(
                &serde_json::to_vec(&serde_json::json!({
                    "location": location,
                    "acceptance": acceptance,
                }))
                .unwrap(),
            )
            .unwrap(),
        )
    };
    vec![
        ("S3", aws, aws_invite),
        ("S3, detected settings", detected, detected_invite),
        ("Google Cloud Storage", gcs, gcs_invite),
        ("Dropbox", dropbox.clone(), account(&dropbox, serde_json::json!("DropboxMount"))),
        (
            "OneDrive",
            onedrive.clone(),
            account(
                &onedrive,
                serde_json::json!({"OneDriveShare": {"token":
                    "u!aHR0cHM6Ly8xZHJ2Lm1zL2YvYy8wMTIzNDU2Nzg5YWJjZGVmL0VhQmNEZUZnSGlKa0xtTm9QcVJzVHVW"}}),
            ),
        ),
        (
            "iCloud",
            icloud.clone(),
            account(
                &icloud,
                serde_json::json!({"CloudKitShare": {"url":
                    "https://www.icloud.com/share/0AbCdEfGhIjKlMnOpQrStUvWx#Household"}}),
            ),
        ),
    ]
}

/// The text lengths of typical codes, which set the size of the QR code a
/// phone's camera has to read: the restore code to write down, the one with
/// the S3 key the person's own device shows, and an invite for Dan.
#[test]
fn typical_codes_keep_their_lengths() {
    let mut lengths = Vec::new();
    for (provider, location, invite_storage) in typical() {
        let restore = |s3_key| {
            RestoreCode {
                store: StoreId(uuid::Uuid::from_u128(1)),
                member: MemberId(uuid::Uuid::from_u128(1)),
                member_keys: coven_crypto::MemberKeys::generate().unwrap(),
                storage: RestoreStorage { location: location.clone(), s3_key }.encode().unwrap(),
            }
            .to_text()
            .unwrap()
            .len()
        };
        let key = match &invite_storage {
            InviteStorage::S3 { credentials, .. } => Some(credentials.clone()),
            InviteStorage::Account(_) => None,
        };
        let keyed = key.map(|key| restore(Some(key)));
        let mut code = invite(invite_storage);
        code.invitee = Some("Dan".into());
        let invite = code.to_text().unwrap();
        decode_code_info(&invite).unwrap();
        lengths.push((provider, restore(None), keyed, invite.len()));
    }
    assert_eq!(
        lengths,
        vec![
            ("S3", 269, Some(378), 423),
            ("S3, detected settings", 341, Some(450), 495),
            ("Google Cloud Storage", 317, Some(492), 537),
            ("Dropbox", 257, None, 303),
            ("OneDrive", 303, None, 489),
            ("iCloud", 372, None, 527),
        ]
    );
}
