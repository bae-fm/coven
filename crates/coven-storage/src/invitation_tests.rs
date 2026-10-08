use super::*;
use serde_json::json;

#[test]
fn invitation_recordings_are_bound_to_a_valid_provider_and_location() {
    let location = StorageConfig::Dropbox {
        namespace_id: "store".into(),
    };
    let invitation = StorageInvitation::for_account(location.clone()).unwrap();
    let recorded = invitation.encode().unwrap();
    let decoded = StorageInvitation::decode(recorded.as_bytes()).unwrap();
    assert_eq!(decoded.location(), &location);
    decoded.check(&location).unwrap();
    assert!(matches!(
        decoded.check(&StorageConfig::Dropbox {
            namespace_id: "other".into()
        }),
        Err(error) if error.failure() == StorageFailure::InvitationMismatch
    ));
    let valid: serde_json::Value = serde_json::from_slice(recorded.as_bytes()).unwrap();
    for acceptance in [
        json!("Granted"),
        json!({"OneDriveShare":{"token":"token"}}),
        json!({"CloudKitShare":{"url":"https://icloud.com/share"}}),
    ] {
        let mut invalid = valid.clone();
        invalid["acceptance"] = acceptance;
        assert!(StorageInvitation::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    let mut invalid = valid;
    invalid["unknown"] = json!(true);
    assert!(StorageInvitation::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
}

#[test]
fn share_tokens_are_validated_and_not_printed() {
    let location = StorageConfig::OneDrive {
        drive_id: "drive".into(),
        folder_id: "folder".into(),
    };
    assert!(StorageInvitation::new(
        location.clone(),
        InvitationAcceptance::OneDriveShare {
            token: SecretText::new(String::new())
        }
    )
    .is_err());
    let invitation = StorageInvitation::new(
        location,
        InvitationAcceptance::OneDriveShare {
            token: SecretText::new("native-secret".into()),
        },
    )
    .unwrap();
    assert!(!format!("{invitation:?}").contains("native-secret"));
    let recorded = invitation.encode().unwrap();
    StorageInvitation::decode(recorded.as_bytes()).unwrap();
    let location = StorageConfig::CloudKit {
        container: "container".into(),
        owner: "owner".into(),
        zone: "zone".into(),
    };
    for url in [
        "",
        "http://icloud.com/share",
        "https://user:password@icloud.com/share",
    ] {
        assert!(StorageInvitation::new(
            location.clone(),
            InvitationAcceptance::CloudKitShare {
                url: SecretText::new(url.into())
            }
        )
        .is_err());
    }
    let invitation = StorageInvitation::new(
        location,
        InvitationAcceptance::CloudKitShare {
            url: SecretText::new("https://icloud.com/share/native-secret#opaque".into()),
        },
    )
    .unwrap();
    assert!(!format!("{invitation:?}").contains("native-secret"));
    StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap();
}
