use super::*;
use coven_crypto::{InviteSecret, MemberKeys, SecretBytes, SecretText};
use coven_foundation::id_source::InviteId;
use coven_storage::{S3Credentials, StorageConfig, StorageInvitation};

fn location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "region".into(),
        endpoint: None,
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
fn metadata_validates_kind_checksum_and_storage_without_exposing_secrets() {
    let restore = RestoreCode {
        store: StoreId(uuid::Uuid::from_u128(1)),
        name: "Household".into(),
        member_keys: MemberKeys::generate().unwrap(),
        storage: RestoreStorage::S3 {
            location: location(),
            credentials: credentials(),
        }
        .encode()
        .unwrap(),
    };
    let restore_text = restore.to_text().unwrap();
    let invite = InviteCode {
        store: restore.store,
        name: restore.name.clone(),
        invite: InviteId(uuid::Uuid::from_u128(2)),
        secret: InviteSecret::generate().unwrap(),
        storage: InviteStorage::S3 {
            invitation: StorageInvitation::for_account(location()).unwrap(),
            credentials: credentials(),
        }
        .encode()
        .unwrap(),
    };
    let invite_text = invite.to_text().unwrap();
    for (text, kind) in [
        (&restore_text, CodeKind::Restore),
        (&invite_text, CodeKind::Invite),
    ] {
        assert_eq!(
            decode_code_info(text).unwrap(),
            CodeInfo {
                kind,
                store_id: restore.store,
                store_name: "Household".into(),
                cloud_provider: CloudProvider::S3,
                needs_oauth: false
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
    let mut invalid = invite;
    invalid.storage = InviteStorage::Account(StorageInvitation::for_account(location()).unwrap())
        .encode()
        .unwrap();
    assert!(matches!(
        decode_code_info(&invalid.to_text().unwrap()),
        Err(CodeError::Invalid)
    ));
}
