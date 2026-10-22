use super::*;
use coven_crypto::{InviteSecret, SecretBytes, SecretText};
use coven_foundation::id_source::{DeviceId, InviteId, KeyId};
use coven_storage::{S3Credentials, StorageConfig, StorageInvitation};

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
            Some(OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::AppFolder,
            )),
        ),
        (
            StorageConfig::Dropbox {
                account_label: Some("ana@work.example".into()),
                location: coven_storage::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
            },
            Some(OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::FullDropbox,
            )),
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
            storage: location.encode().unwrap(),
        }
        .to_text()
        .unwrap();
        let CodeInfo::Restore(metadata) = decode_code_info(&code).unwrap() else {
            panic!("restore metadata");
        };
        assert_eq!(metadata.oauth_access, access);
        assert_eq!(metadata.cloud_provider, location.provider());
        assert_eq!(metadata.location, location);
        let decoded = read_restore_code(&code).unwrap();
        assert_eq!(
            StorageConfig::decode(decoded.storage.as_bytes()).unwrap(),
            location
        );
    }
}

#[test]
fn metadata_validates_kind_checksum_and_storage_without_exposing_secrets() {
    let restore = RestoreCode {
        store: StoreId(uuid::Uuid::from_u128(1)),
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: location().encode().unwrap(),
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
            };
            match kind {
                CodeKind::Restore => CodeInfo::Restore(metadata),
                CodeKind::Invite => CodeInfo::Invite {
                    metadata,
                    store_name: "Household".into(),
                },
            }
        });
        assert!(!format!("{:?}", decode_code_info(text).unwrap()).contains("secret"));
        let truncated = &text[..text.len() - 1];
        assert!(matches!(
            decode_code_info(truncated),
            Err(CodeError::Invalid)
        ));
    }
    assert!(matches!(
        read_invite_code(&restore_text),
        Err(CodeError::WrongKind {
            expected: CodeKind::Invite,
            actual: CodeKind::Restore
        })
    ));
    assert!(matches!(
        read_restore_code(&invite_text),
        Err(CodeError::WrongKind {
            expected: CodeKind::Restore,
            actual: CodeKind::Invite
        })
    ));
    let mut invalid = restore;
    invalid.storage = SecretBytes::new(b"invalid payload".to_vec());
    assert!(matches!(
        decode_code_info(&invalid.to_text().unwrap()),
        Err(CodeError::Invalid)
    ));
    invalid.storage = SecretBytes::new(br#"{"S3":{"location":{"S3":{"bucket":"bucket","region":"region","endpoint":null,"profile":null,"prefix":"store"}},"credentials":{"access_key_id":"access","secret_access_key":"secret"}}}"#.to_vec());
    assert!(matches!(
        read_restore_code(&invalid.to_text().unwrap()),
        Err(CodeError::Invalid)
    ));
    let mut invalid = invite;
    invalid.storage = InviteStorage::Account(StorageInvitation::for_account(location()).unwrap())
        .encode()
        .unwrap();
    assert!(matches!(
        decode_code_info(&invalid.to_text().unwrap()),
        Err(CodeError::Invalid)
    ));
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
        assert!(!format!("{info:?}").contains("provider-admission-secret"));
        assert_eq!(
            info.metadata().location.encode().unwrap().as_bytes(),
            location.encode().unwrap().as_bytes()
        );
    }
}
