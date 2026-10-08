use super::*;
use crate::OAuthTokens;
use coven_crypto::SecretText;

#[test]
fn account_restore_codes_contain_location_without_device_tokens() {
    for location in [
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        },
        StorageConfig::Dropbox {
            namespace_id: "folder".into(),
        },
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "folder".into(),
        },
    ] {
        let data = ConnectionCredentials {
            location: location.clone(),
            credentials: StorageCredentials::OAuth(OAuthTokens {
                access_token: SecretText::new("private-access-token".into()),
                refresh_token: Some(SecretText::new("private-refresh-token".into())),
                expires_at: None,
            }),
        };
        let bytes = RestoreStorage::from_connection(&data).encode().unwrap();
        assert!(!std::str::from_utf8(bytes.as_bytes())
            .unwrap()
            .contains("token"));
        let restored = RestoreStorage::decode(bytes.as_bytes()).unwrap();
        assert_eq!(restored.location(), &location);
        assert!(matches!(restored, RestoreStorage::Account(_)));
    }
}
