use super::*;
use crate::invitation::InvitationAcceptance;
use crate::{DropboxLocation, StorageConfig};
use coven_crypto::SecretText;

fn s3_location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "region".into(),
        endpoint: None,
        profile: None,
    }
}

fn key() -> S3Credentials {
    S3Credentials {
        access_key_id: "id".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

fn onedrive() -> StorageConfig {
    StorageConfig::OneDrive {
        drive_id: "drive".into(),
        folder_id: "folder".into(),
        account_label: Some("ana@example.com".into()),
        folder_label: None,
    }
}

fn cloudkit() -> StorageConfig {
    StorageConfig::CloudKit {
        container: "iCloud.fm.bae".into(),
        owner: "_owner".into(),
        zone: "store".into(),
    }
}

fn dan() -> crate::SharedWith {
    crate::SharedWith::new("dan@example.com", Some("dbid:dan".into())).unwrap()
}

/// An invite of every provider and acceptance.
fn invites() -> Vec<InviteStorage> {
    vec![
        InviteStorage::S3 {
            invitation: StorageInvitation::for_account(s3_location()).unwrap(),
            credentials: key(),
        },
        InviteStorage::Account(
            StorageInvitation::for_account(StorageConfig::Dropbox {
                location: DropboxLocation::SharedFolder { namespace_id: "12345678901".into() },
                account_label: Some("ana@example.com".into()),
                folder_label: None,
            })
            .unwrap()
            .shared_with(dan()),
        ),
        InviteStorage::Account(
            StorageInvitation::for_account(onedrive()).unwrap().shared_with(dan()),
        ),
        InviteStorage::Account(
            StorageInvitation::new(
                onedrive(),
                InvitationAcceptance::OneDriveShare {
                    token: SecretText::new("u!share-token".into()),
                },
            )
            .unwrap()
            .shared_with(crate::SharedWith::new("dan@example.com", None).unwrap()),
        ),
        InviteStorage::Account(
            StorageInvitation::new(
                cloudkit(),
                InvitationAcceptance::CloudKitShare {
                    url: SecretText::new("https://www.icloud.com/share/0abc#store".into()),
                },
            )
            .unwrap(),
        ),
    ]
}

#[test]
fn every_providers_invite_round_trips_canonically() {
    for invite in invites() {
        let bytes = invite.encode().unwrap();
        assert_eq!(bytes.capacity(), bytes.as_bytes().len());
        let decoded = InviteStorage::decode(bytes.as_bytes()).unwrap();
        assert_eq!(decoded.encode().unwrap().as_bytes(), bytes.as_bytes());
        let (expected, actual) = match (&invite, &decoded) {
            (InviteStorage::Account(expected), InviteStorage::Account(actual)) => {
                (expected, actual)
            }
            (
                InviteStorage::S3 { invitation: expected, .. },
                InviteStorage::S3 { invitation: actual, credentials },
            ) => {
                assert_eq!(credentials.access_key_id, "id");
                assert_eq!(credentials.secret_access_key.as_str(), "secret");
                (expected, actual)
            }
            _ => panic!("the invite changed kind"),
        };
        assert_eq!(actual.location(), expected.location());
        assert_eq!(actual.account(), expected.account());
        let acceptance = |invitation: &StorageInvitation| match &invitation.acceptance {
            InvitationAcceptance::Granted => "granted".to_owned(),
            InvitationAcceptance::DropboxMount => "mount".to_owned(),
            InvitationAcceptance::OneDriveShare { token } => token.as_str().to_owned(),
            InvitationAcceptance::CloudKitShare { url } => url.as_str().to_owned(),
        };
        assert_eq!(acceptance(actual), acceptance(expected));
    }
}

#[test]
fn truncated_trailing_and_changed_invites_are_refused_or_canonical() {
    for invite in invites() {
        let bytes = invite.encode().unwrap();
        let bytes = bytes.as_bytes();
        let reencode = |bytes: &[u8]| -> Result<SecretBytes, StorageError> {
            InviteStorage::decode(bytes)?.encode()
        };
        for end in 0..bytes.len() {
            assert_eq!(
                reencode(&bytes[..end]).err().unwrap().failure(),
                StorageFailure::Encoding,
                "{end}"
            );
        }
        let mut changed = bytes.to_vec();
        changed.push(0);
        assert_eq!(reencode(&changed).err().unwrap().failure(), StorageFailure::Encoding);
        changed.pop();
        for index in 0..changed.len() {
            for bit in 0..8 {
                changed[index] ^= 1 << bit;
                if let Ok(encoded) = reencode(&changed) {
                    assert_eq!(encoded.as_bytes(), changed);
                }
                changed[index] ^= 1 << bit;
            }
        }
    }
}

#[test]
fn the_provider_fixes_what_follows_the_location() {
    let location = |invite: &InviteStorage| {
        let bytes = coven_format::code_fields::pack(|out| match invite {
            InviteStorage::Account(invitation) | InviteStorage::S3 { invitation, .. } => {
                invitation.location().pack(out)
            }
        })
        .unwrap();
        bytes.as_bytes().len()
    };
    let invites = invites();
    let tails: Vec<Vec<u8>> = invites
        .iter()
        .map(|invite| invite.encode().unwrap().as_bytes()[location(invite)..].to_vec())
        .collect();
    // S3: the key; Dropbox: the shared-with account; OneDrive: no share token,
    // or one, then the shared-with account; CloudKit: the share URL.
    assert_eq!(tails[0], b"\0\0\0\x02id\0\0\0\x06secret");
    assert_eq!(tails[1], b"\0\0\0\x0fdan@example.com\x01\0\0\0\x08dbid:dan");
    assert_eq!(tails[2], b"\0\0\0\0\x0fdan@example.com\x01\0\0\0\x08dbid:dan");
    assert_eq!(tails[3], b"\x01\0\0\0\x0du!share-token\0\0\0\x0fdan@example.com\0");
    assert_eq!(tails[4], b"\0\0\0\x27https://www.icloud.com/share/0abc#store");
}

#[test]
fn an_s3_invite_carries_exactly_its_key() {
    let account = InviteStorage::Account(StorageInvitation::for_account(s3_location()).unwrap());
    assert_eq!(account.encode().err().unwrap().failure(), StorageFailure::InvalidConfiguration);
    let misplaced = InviteStorage::S3 {
        invitation: StorageInvitation::for_account(onedrive()).unwrap(),
        credentials: key(),
    };
    assert_eq!(misplaced.encode().err().unwrap().failure(), StorageFailure::InvalidConfiguration);
    let empty = InviteStorage::S3 {
        invitation: StorageInvitation::for_account(s3_location()).unwrap(),
        credentials: S3Credentials {
            access_key_id: "id".into(),
            secret_access_key: SecretText::new(String::new()),
        },
    };
    assert_eq!(empty.encode().err().unwrap().failure(), StorageFailure::InvalidConfiguration);
    // An S3 location with no key after it is truncated.
    let location = coven_format::code_fields::pack(|out| s3_location().pack(out)).unwrap();
    assert_eq!(
        InviteStorage::decode(location.as_bytes()).err().unwrap().failure(),
        StorageFailure::Encoding
    );
}

#[test]
fn an_app_folder_or_an_empty_share_never_decodes() {
    let app_folder = coven_format::code_fields::pack(|out| {
        StorageConfig::Dropbox {
            location: DropboxLocation::AppFolder,
            account_label: None,
            folder_label: None,
        }
        .pack(out)
    })
    .unwrap();
    assert_eq!(
        InviteStorage::decode(app_folder.as_bytes()).err().unwrap().failure(),
        StorageFailure::DropboxAppFolderNotShareable
    );
    let mut empty_token =
        coven_format::code_fields::pack(|out| onedrive().pack(out)).unwrap().as_bytes().to_vec();
    empty_token.extend_from_slice(b"\x01\0\0\0\0");
    assert_eq!(
        InviteStorage::decode(&empty_token).err().unwrap().failure(),
        StorageFailure::InvalidConfiguration
    );
}

#[test]
fn the_format_fixtures_storage_part_is_this_encoding() {
    let fixture = coven_format::test_utils::invite();
    let decoded = InviteStorage::decode(fixture.storage.as_bytes()).unwrap();
    assert!(
        matches!(&decoded, InviteStorage::S3 { invitation, .. } if invitation.location() == &s3_location())
    );
    assert_eq!(decoded.encode().unwrap().as_bytes(), fixture.storage.as_bytes());
}

#[test]
fn dropbox_and_onedrive_invites_carry_the_account_and_no_other_does() {
    let dropbox = || {
        StorageInvitation::for_account(StorageConfig::Dropbox {
            location: DropboxLocation::SharedFolder { namespace_id: "1".into() },
            account_label: None,
            folder_label: None,
        })
        .unwrap()
    };
    let refused = |storage: InviteStorage| {
        assert_eq!(storage.encode().err().unwrap().failure(), StorageFailure::InvalidConfiguration);
    };
    refused(InviteStorage::Account(dropbox()));
    refused(InviteStorage::Account(StorageInvitation::for_account(onedrive()).unwrap()));
    refused(InviteStorage::S3 {
        invitation: StorageInvitation::for_account(s3_location()).unwrap().shared_with(dan()),
        credentials: key(),
    });
    refused(InviteStorage::Account(
        StorageInvitation::new(
            cloudkit(),
            InvitationAcceptance::CloudKitShare {
                url: SecretText::new("https://www.icloud.com/share/0abc#store".into()),
            },
        )
        .unwrap()
        .shared_with(dan()),
    ));
}

#[test]
fn a_reader_refuses_an_account_in_another_spelling() {
    let invite = InviteStorage::Account(
        StorageInvitation::for_account(onedrive())
            .unwrap()
            .shared_with(crate::SharedWith::new("dan@example.com", None).unwrap()),
    );
    let bytes = invite.encode().unwrap();
    let bytes = bytes.as_bytes();
    // The address is the last text before the id's absence tag.
    let at = bytes.windows(15).position(|window| window == b"dan@example.com").unwrap();
    let mut shouted = bytes.to_vec();
    shouted[at..at + 3].copy_from_slice(b"DAN");
    assert_eq!(InviteStorage::decode(&shouted).err().unwrap().failure(), StorageFailure::Encoding);
}
