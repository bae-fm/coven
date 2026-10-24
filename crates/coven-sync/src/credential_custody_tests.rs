use super::*;
use coven_crypto::{custody::Keychain, SecretText};
use coven_foundation::id_source::StoreId;
use coven_storage::{OAuthTokens, S3Credentials, StorageConfig};
use std::sync::Arc;

fn key() -> StorageCredentials {
    StorageCredentials::S3(S3Credentials {
        access_key_id: "replacement-id".into(),
        secret_access_key: SecretText::new("replacement-secret".into()),
    })
}

fn household() -> SavedName {
    SavedName {
        name: "Household".into(),
        named_at: coven_merge::Slot(1),
    }
}

/// The saved entry's code bytes and name.
fn saved(store: &CredentialKeychains) -> (Vec<u8>, String) {
    let bytes = store.synced.restore_code().unwrap().unwrap();
    let (saved, _) = crate::read_saved_code(store.synced.store(), bytes.as_bytes()).unwrap();
    (saved.code.to_bytes().unwrap().to_vec(), saved.name)
}

fn fixture(present: bool) -> (Arc<Keychain>, CredentialKeychains, RestoreCode) {
    let keychain = Keychain::in_memory("credential-commit").unwrap();
    let id = StoreId(uuid::Uuid::from_u128(1));
    let store = CredentialKeychains {
        device: Arc::new(StoreKeychain::new(keychain.clone(), id)),
        synced: Arc::new(SyncedKeychain::new(keychain.clone(), id)),
    };
    if present {
        // Synced and local values may differ; compensation must preserve each.
        store
            .device
            .set_storage_credentials(&SecretBytes::new(b"prior-device".to_vec()))
            .unwrap();
        store
            .synced
            .set_s3_credentials(&SecretBytes::new(b"prior-synced-key".to_vec()))
            .unwrap();
        store
            .synced
            .set_restore_code(&SecretBytes::new(b"prior-code".to_vec()))
            .unwrap();
    }
    let code = RestoreCode {
        store: id,
        member: coven_foundation::id_source::MemberId(uuid::Uuid::from_u128(1)),
        member_keys: coven_crypto::MemberKeys::generate().unwrap(),
        storage: StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "region".into(),
            endpoint: None,
            profile: None,
            prefix: coven_storage::S3Prefix::parse("store").unwrap(),
        }
        .encode()
        .unwrap(),
    };
    (keychain, store, code)
}

fn assert_prior(store: &CredentialKeychains, present: bool) {
    for (actual, expected) in [
        (
            store.device.storage_credentials().unwrap(),
            b"prior-device".as_slice(),
        ),
        (
            store.synced.s3_credentials().unwrap(),
            b"prior-synced-key".as_slice(),
        ),
        (
            store.synced.restore_code().unwrap(),
            b"prior-code".as_slice(),
        ),
    ] {
        assert_eq!(
            actual.as_ref().map(SecretBytes::as_bytes),
            present.then_some(expected)
        );
    }
}

#[test]
fn every_custody_read_and_write_failure_preserves_the_exact_prior_values() {
    for present in [false, true] {
        for with_code in [false, true] {
            // Saving the code reads the entry again before writing it.
            let count = if with_code { 7 } else { 4 };
            let name = household();
            for after in 0..count {
                let (keychain, store, code) = fixture(present);
                keychain.fail_operations_after(&[after]);
                assert!(
                    matches!(
                        commit_credentials(&store, &key(), with_code.then_some((&code, &name))),
                        Err(SyncError::SecureStorage(_))
                    ),
                    "after {after}, with code {with_code}"
                );
                assert_prior(&store, present);
                let rollback =
                    commit_credentials(&store, &key(), with_code.then_some((&code, &name)))
                        .unwrap();
                let StorageCredentials::S3(device) = StorageCredentials::decode(
                    store
                        .device
                        .storage_credentials()
                        .unwrap()
                        .unwrap()
                        .as_bytes(),
                )
                .unwrap() else {
                    panic!("S3 device credentials")
                };
                let synced = S3Credentials::decode(
                    store.synced.s3_credentials().unwrap().unwrap().as_bytes(),
                )
                .unwrap();
                assert_eq!(device.access_key_id, "replacement-id");
                assert_eq!(device.secret_access_key.as_str(), "replacement-secret");
                assert_eq!(synced.access_key_id, device.access_key_id);
                assert_eq!(
                    synced.secret_access_key.as_str(),
                    device.secret_access_key.as_str()
                );
                if with_code {
                    assert_eq!(
                        saved(&store),
                        (code.to_bytes().unwrap().to_vec(), "Household".into())
                    );
                }
                rollback.restore(&store).unwrap();
                assert_prior(&store, present);
            }
        }
    }
}

#[test]
fn commit_and_rollback_failures_both_reach_the_initiator_and_retry_commits_every_value() {
    for present in [false, true] {
        for cleanup in 0..3 {
            let (keychain, store, code) = fixture(present);
            let name = household();
            // Read all three entries; write device and S3; refuse code, then
            // refuse each compensation in turn without skipping the others.
            keychain.fail_operations_after(&[5, cleanup]);
            assert!(matches!(
                commit_credentials(&store, &key(), Some((&code, &name))),
                Err(SyncError::Cleanup { operation, cleanup })
                    if matches!(*operation, SyncError::SecureStorage(_))
                        && matches!(*cleanup, SyncError::SecureStorage(_))
            ));
            let restored = [
                store.synced.restore_code().unwrap(),
                store.synced.s3_credentials().unwrap(),
                store.device.storage_credentials().unwrap(),
            ];
            for (index, (actual, expected)) in restored
                .iter()
                .zip([
                    b"prior-code".as_slice(),
                    b"prior-synced-key",
                    b"prior-device",
                ])
                .enumerate()
            {
                if index != cleanup {
                    assert_eq!(
                        actual.as_ref().map(SecretBytes::as_bytes),
                        present.then_some(expected)
                    );
                }
            }
            commit_credentials(&store, &key(), Some((&code, &name))).unwrap();
            assert_eq!(
                store
                    .device
                    .storage_credentials()
                    .unwrap()
                    .unwrap()
                    .as_bytes(),
                key().encode().unwrap().as_bytes()
            );
            assert_eq!(
                saved(&store),
                (code.to_bytes().unwrap().to_vec(), "Household".into())
            );
            let StorageCredentials::S3(keys) = key() else {
                unreachable!()
            };
            assert_eq!(
                store.synced.s3_credentials().unwrap().unwrap().as_bytes(),
                keys.encode().unwrap().as_bytes()
            );
        }
    }
}

#[test]
fn switching_to_account_storage_removes_the_separate_s3_key_and_can_roll_back() {
    let (keychain, store, mut code) = fixture(true);
    code.storage = StorageConfig::Dropbox {
        account_label: None,
        location: coven_storage::DropboxLocation::SharedFolder {
            namespace_id: "folder".into(),
        },
    }
    .encode()
    .unwrap();
    let oauth = StorageCredentials::OAuth(OAuthTokens {
        client_id: "dropbox".into(),
        access: coven_storage::OAuthAccess::Dropbox(coven_storage::DropboxAccess::FullDropbox),
        access_token: SecretText::new("account-token".into()),
        refresh_token: None,
    });
    let name = household();
    let rollback = commit_credentials(&store, &oauth, Some((&code, &name))).unwrap();
    assert!(store.synced.s3_credentials().unwrap().is_none());
    rollback.restore(&store).unwrap();
    assert_prior(&store, true);
    for after in 0..7 {
        keychain.fail_operations_after(&[after]);
        assert!(matches!(
            commit_credentials(&store, &oauth, Some((&code, &name))),
            Err(SyncError::SecureStorage(_))
        ));
        assert_prior(&store, true);
    }
}

#[test]
fn oauth_commit_and_compensation_touch_only_device_credentials() {
    let (keychain, store, _) = fixture(true);
    let oauth = StorageCredentials::OAuth(OAuthTokens {
        client_id: "dropbox".into(),
        access: coven_storage::OAuthAccess::Dropbox(coven_storage::DropboxAccess::FullDropbox),
        access_token: SecretText::new("new-access".into()),
        refresh_token: Some(SecretText::new("new-refresh".into())),
    });
    // Exactly one read and one write: a third operation would fail.
    keychain.fail_operations_after(&[2]);
    let rollback = commit_credentials(&store, &oauth, None).unwrap();
    assert!(store.device.storage_credentials().is_err());
    assert_eq!(
        store.synced.restore_code().unwrap().unwrap().as_bytes(),
        b"prior-code"
    );
    assert_eq!(
        store.synced.s3_credentials().unwrap().unwrap().as_bytes(),
        b"prior-synced-key"
    );
    keychain.fail_operations_after(&[1]);
    rollback.restore(&store).unwrap();
    assert!(store.device.storage_credentials().is_err());
    assert_prior(&store, true);
}

#[test]
fn replacing_s3_credentials_never_reads_or_rewrites_the_code() {
    let (keychain, store, _) = fixture(true);
    // Two reads and two writes, independent of the code's validity or presence.
    keychain.fail_operations_after(&[4]);
    let rollback = commit_credentials(&store, &key(), None).unwrap();
    assert!(store.device.storage_credentials().is_err());
    assert_eq!(
        store.synced.restore_code().unwrap().unwrap().as_bytes(),
        b"prior-code"
    );
    rollback.restore(&store).unwrap();
    assert_prior(&store, true);
}

/// A join publishes its restore code beside the sign-in it kept: only the
/// synced entries change, a failure at any step restores them, and rolling
/// back leaves the kept device credentials as they are (§13.2).
#[test]
fn a_joins_code_commits_beside_its_kept_credentials_and_rolls_back_only_what_it_changed() {
    for present in [false, true] {
        let name = household();
        // Reads S3 and code, deletes S3, then saving the code reads it again
        // and writes it.
        for after in 0..5 {
            let (keychain, store, code) = fixture(present);
            keychain.fail_operations_after(&[after]);
            assert!(
                matches!(
                    commit_account_code(&store, &code, &name),
                    Err(SyncError::SecureStorage(_))
                ),
                "after {after}"
            );
            assert_prior(&store, present);
        }
        let (keychain, store, code) = fixture(present);
        let rollback = commit_account_code(&store, &code, &name).unwrap();
        assert!(store.synced.s3_credentials().unwrap().is_none());
        assert_eq!(
            saved(&store),
            (code.to_bytes().unwrap().to_vec(), "Household".into())
        );
        // The device's own entry is never read or written: a failing call on
        // it would fail the rollback.
        keychain.fail_operations_after(&[2]);
        rollback.restore(&store).unwrap();
        keychain.fail_operations_after(&[]);
        assert_prior(&store, present);
    }
}
