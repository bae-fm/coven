use super::*;
use coven_crypto::{InviteSecret, SecretBytes, SecretText};
use coven_foundation::id_source::{DeviceId, InviteId, KeyId};
use coven_storage::{S3Credentials, StorageConfig, StorageInvitation};

fn location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "region".into(),
        endpoint: None,
        profile: None,
        prefix: "store".into(),
    }
}

fn credentials() -> S3Credentials {
    S3Credentials {
        access_key_id: "access".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

#[test]
fn restore_metadata_retains_the_creation_access_choice() {
    for (location, access) in [
        (
            StorageConfig::Dropbox {
                location: coven_storage::DropboxLocation::AppFolder,
            },
            OAuthAccess::Dropbox(coven_storage::DropboxAccess::AppFolder),
        ),
        (
            StorageConfig::Dropbox {
                location: coven_storage::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
            },
            OAuthAccess::Dropbox(coven_storage::DropboxAccess::FullDropbox),
        ),
    ] {
        let code = RestoreCode {
            store: StoreId(uuid::Uuid::from_u128(1)),
            name: "Personal".into(),
            member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
            member_keys: coven_crypto::MemberKeys::generate().unwrap(),
            storage: location.encode().unwrap(),
        }
        .to_text()
        .unwrap();
        assert_eq!(decode_code_info(&code).unwrap().oauth_access, Some(access));
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
        name: "Household".into(),
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: location().encode().unwrap(),
    };
    let restore_text = restore.to_text().unwrap();
    let invite = InviteCode {
        store: restore.store,
        name: restore.name.clone(),
        invite: InviteId(uuid::Uuid::from_u128(2)),
        // The admin's chosen id for the invited person, not the inviter's.
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(2)),
        secret: InviteSecret::generate().unwrap(),
        initial_key: KeyId(uuid::Uuid::from_u128(3)),
        inviting_writer: DeviceId(4),
        read_through: 12,
        storage: InviteStorage::S3 {
            invitation: StorageInvitation::for_account(location()).unwrap(),
            credentials: credentials(),
        }
        .encode()
        .unwrap(),
    };
    let invite_text = invite.to_text().unwrap();
    let decoded = read_invite_code(&invite_text).unwrap();
    assert_eq!(decoded.initial_key, invite.initial_key);
    assert_eq!(decoded.inviting_writer, invite.inviting_writer);
    assert_eq!(decoded.read_through, invite.read_through);
    for (text, kind, member) in [
        (&restore_text, CodeKind::Restore, restore.member),
        (&invite_text, CodeKind::Invite, invite.member),
    ] {
        assert_eq!(
            decode_code_info(text).unwrap(),
            CodeInfo {
                kind,
                store_id: restore.store,
                store_name: "Household".into(),
                member,
                cloud_provider: CloudProvider::S3,
                oauth_access: None
            }
        );
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
