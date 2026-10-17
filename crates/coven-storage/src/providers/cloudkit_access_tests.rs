use super::*;
use crate::providers::cloudkit::tests::live_zone;
use coven_foundation::id_source::{DeviceId, IdSourceRef, StoreId, UuidIds};
use std::io::BufRead;

// Stderr prompts are the interactive test protocol, not diagnostic logs.
// Each step is invoked explicitly from an entitled host. The invitation file
// contains a secret share URL; the owner creates it with owner-only permissions.
#[tokio::test]
#[ignore = "requires two signed hosts using different iCloud accounts and manual handoff"]
async fn cloudkit_live_sharing() {
    let role = std::env::var("COVEN_CLOUDKIT_TEST_ROLE").expect("owner or recipient role");
    let email = std::env::var("COVEN_CLOUDKIT_TEST_EMAIL").expect("recipient's iCloud email");
    let handoff =
        std::env::var("COVEN_CLOUDKIT_TEST_INVITATION").expect("private invitation file path");
    let ids: IdSourceRef = Arc::new(UuidIds);
    match role.as_str() {
        "owner" => {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let store = StoreId(ids.new_id());
            let native = Arc::new(
                live_zone::prepare(store, "cloudkit_live_sharing", ids)
                    .await
                    .unwrap(),
            );
            let storage = StorageConnection::from_provider(native.clone());
            let object = ObjectPath::positions(store, DeviceId(7));
            storage.replace(&object, b"owner").await.unwrap();
            let AccessGrant::Granted { invitation } = storage.grant_access(&email).await.unwrap()
            else {
                panic!("CloudKit must grant an account");
            };
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&handoff)
                .unwrap();
            let data = serde_json::to_vec(&(store, invitation)).unwrap();
            file.write_all(&data).unwrap();
            file.sync_all().unwrap();
            eprintln!("Transfer the private invitation file to the recipient host and run its test. Enter accepted after it verifies shared access.");
            let mut lines = std::io::stdin().lock().lines();
            assert_eq!(lines.next().unwrap().unwrap(), "accepted");
            assert_eq!(storage.read(&object).await.unwrap(), b"recipient");
            assert!(storage.revoke_access(&email).await.unwrap().is_empty());
            assert_eq!(storage.read(&object).await.unwrap(), b"recipient");
            eprintln!("Tell the recipient to enter revoked. Enter verified after that host verifies access refusal.");
            assert_eq!(lines.next().unwrap().unwrap(), "verified");
            live_zone::delete(&native).await.unwrap();
            std::fs::remove_file(handoff).unwrap();
        }
        "recipient" => {
            let (store, invitation): (StoreId, StorageInvitation) =
                serde_json::from_slice(&std::fs::read(&handoff).unwrap()).unwrap();
            let native = CloudKitStorage::new(invitation.location().clone(), ids)
                .await
                .unwrap();
            let storage = StorageConnection::from_provider(Arc::new(native));
            storage.join(store, &invitation).await.unwrap();
            storage.join(store, &invitation).await.unwrap();
            assert!(storage
                .account()
                .await
                .unwrap()
                .eq_ignore_ascii_case(&email));
            let object = ObjectPath::positions(store, DeviceId(7));
            assert_eq!(storage.read(&object).await.unwrap(), b"owner");
            storage.replace(&object, b"recipient").await.unwrap();
            eprintln!("Tell the owner to enter accepted. Enter revoked after the owner saves access removal.");
            assert_eq!(
                std::io::stdin().lock().lines().next().unwrap().unwrap(),
                "revoked"
            );
            let error = storage.read(&object).await.unwrap_err();
            assert!(matches!(
                error.failure(),
                StorageFailure::PermissionDenied
                    | StorageFailure::ContainerNotFound
                    | StorageFailure::NotFound
            ));
            std::fs::remove_file(handoff).unwrap();
        }
        _ => panic!("COVEN_CLOUDKIT_TEST_ROLE must be owner or recipient"),
    }
}

fn seen(id: Option<&str>, email: Option<&str>, owner: bool, participant_id: &str) -> Seen {
    Seen {
        id: id.map(str::to_owned),
        email: email.map(str::to_owned),
        owner,
        participant_id: participant_id.to_owned(),
    }
}

/// A public share keeps reaching everyone while it stays public; the member
/// is still removed from it, so turning public access off cuts them off.
#[test]
fn revoking_from_a_public_share_removes_the_participant_and_reports_public_access() {
    let participants = [
        seen(Some("_owner"), None, true, "p-owner"),
        seen(Some("_ben"), Some("ben@example.test"), false, "p-ben"),
        seen(None, Some("cy@example.test"), false, "p-cy"),
        seen(None, None, false, "p-unknown"),
    ];
    for public in [false, true] {
        let (removed, retained) = revocation(
            "share",
            public,
            &participants,
            Some("_ben"),
            "ben@example.test",
        );
        assert_eq!(removed, [1]);
        let mut expected = vec![RetainedAccess {
            provider_id: "p-unknown".into(),
            reason: RetainedAccessReason::UnidentifiedAccount,
        }];
        if public {
            expected.insert(
                0,
                RetainedAccess {
                    provider_id: "share".into(),
                    reason: RetainedAccessReason::OtherAccounts,
                },
            );
        }
        assert_eq!(retained, expected);
    }
    // A participant not yet resolved to a user record matches by email.
    let (removed, _) = revocation("share", true, &participants, None, "CY@example.test");
    assert_eq!(removed, [2]);
}

#[test]
fn a_share_from_another_location_names_each_differing_part_and_both_values() {
    let identity = |owner: &str, record: &str| ShareIdentity {
        container: "iCloud.example".into(),
        zone: "coven-zone".into(),
        owner: owner.into(),
        record: record.into(),
    };
    let expected = identity("_owner", "cloudkit.zoneshare");
    assert_eq!(
        identity("_owner", "cloudkit.zoneshare").mismatch(&expected),
        None
    );
    assert_eq!(
        identity("_other", "cloudkit.zoneshare").mismatch(&expected),
        Some(r#"share metadata zone owner name "_other", invitation expects "_owner""#.into())
    );
    assert_eq!(
        identity("_other", "share").mismatch(&expected),
        Some(
            r#"share metadata zone owner name "_other", invitation expects "_owner"; share metadata share record name "share", invitation expects "cloudkit.zoneshare""#
                .into()
        )
    );
}
