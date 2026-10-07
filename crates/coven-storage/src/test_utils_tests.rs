use super::*;
fn config() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: None,
        prefix: "store".into(),
    }
}
#[tokio::test]
async fn memory_conforms() {
    Conformance::new(Arc::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ))
    .run()
    .await
    .unwrap();
}
#[tokio::test]
async fn create_once_accepts_an_occupied_path_without_replacing_it() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    provider.create(&path, b"stored").await.unwrap();
    provider.create_once(&path, b"retry").await.unwrap();
    assert_eq!(provider.read(&path).await.unwrap(), b"stored");
}
#[tokio::test]
async fn crash_after_accepted_part_and_dropped_part_resumes() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::file(
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xaa; 16])),
    );
    let mut session = provider.begin_upload(&path, 10).await.unwrap();
    provider.upload_part(&mut session, b"abcd").await.unwrap();
    let recorded = session.encode().unwrap();
    provider
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(provider.upload_part(&mut session, b"efgh").await.is_err());
    assert_eq!(session.confirmed_bytes(), 4);
    drop(session);
    let reopened = provider.clone();
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened
        .set_faults(Faults {
            drop_part: true,
            ..Faults::none()
        })
        .await;
    assert!(reopened.upload_part(&mut session, b"ij").await.is_err());
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened.upload_part(&mut session, b"ij").await.unwrap();
    let before_finish = session.encode().unwrap();
    reopened.finish_upload(&mut session).await.unwrap();
    let mut lost_finish = UploadSession::decode(before_finish.as_bytes()).unwrap();
    reopened.finish_upload(&mut lost_finish).await.unwrap();
    assert_eq!(reopened.read(&path).await.unwrap(), b"abcdefghij");
}
#[tokio::test]
async fn setup_refuses_other_store_and_retries_its_own_entry() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    assert_eq!(
        provider
            .setup(&path, b"other store")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
}
#[tokio::test(start_paused = true)]
async fn faults_are_counted_and_delay_is_awaited() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    provider
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::QuotaExceeded,
            delay: Duration::from_secs(3),
            ..Faults::none()
        })
        .await;
    let start = tokio::time::Instant::now();
    for _ in 0..2 {
        assert_eq!(
            provider
                .list(&ObjectPrefix::all())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::QuotaExceeded
        );
    }
    assert!(provider
        .list(&ObjectPrefix::all())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(start.elapsed(), Duration::from_secs(9));
}

#[tokio::test]
async fn expired_upload_restarts_with_a_new_recording_at_the_same_destination() {
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut expired = storage.begin_upload(&path, 5).await.unwrap();
    storage.upload_part(&mut expired, b"abcd").await.unwrap();
    storage.abort_upload(&expired).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut expired).await,
        Err(StorageError::SessionExpired)
    ));
    let replacement = storage.restart_upload(&expired).await.unwrap();
    assert_eq!(replacement.path(), &path);
    assert_eq!(replacement.total_bytes(), 5);
    assert_eq!(replacement.confirmed_bytes(), 0);
    assert_ne!(
        replacement.encode().unwrap().as_bytes(),
        expired.encode().unwrap().as_bytes()
    );
    let mut replacement = UploadSession::decode(replacement.encode().unwrap().as_bytes()).unwrap();
    storage
        .upload_part(&mut replacement, b"abcd")
        .await
        .unwrap();
    storage.upload_part(&mut replacement, b"e").await.unwrap();
    storage.finish_upload(&mut replacement).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"abcde");
    assert!(matches!(
        storage.restart_upload(&replacement).await,
        Err(StorageError::InvalidPart)
    ));
    let other = MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "elsewhere".into(),
        },
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    assert!(matches!(
        other.restart_upload(&expired).await,
        Err(StorageError::SessionMismatch)
    ));
}

#[tokio::test]
async fn failed_automatic_upload_is_aborted_without_publishing() {
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    storage
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(
        storage.create(&path, &[1; 17]).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    assert!(storage.state.lock().await.uploads.is_empty());
    storage.create(&path, &[1; 17]).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), [1; 17]);
}

#[tokio::test]
async fn sharing_authority_belongs_to_the_adapters_account() {
    for config in sharing_configs() {
        let owner = MemoryStorage::new(
            config,
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap();
        owner.grant_access("kept@example.test").await.unwrap();
        let recipient = MemoryStorage::for_recipient(&owner, "kept@example.test").unwrap();
        assert!(matches!(
            recipient.grant_access("new@example.test").await,
            Err(StorageError::NotStoreOwner)
        ));
        assert!(matches!(
            recipient
                .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
                .await,
            Err(StorageError::NotStoreOwner)
        ));
        assert_eq!(
            owner
                .state
                .lock()
                .await
                .accounts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            ["kept@example.test"]
        );
        owner
            .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
            .await
            .unwrap();
        assert!(owner.state.lock().await.accounts.is_empty());
    }
    let s3 = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    assert!(matches!(
        s3.grant_access("member").await.unwrap(),
        AccessGrant::CreateAccessKey
    ));
    assert!(matches!(
        s3.revoke_access(&MemberAccess::S3AccessKey {
            access_key_id: "key".into()
        })
        .await
        .unwrap(),
        MemberRemoval::DeleteAccessKey { .. }
    ));
}

#[tokio::test]
async fn reconnect_accepts_its_first_entry_after_the_store_has_uploaded_more() {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.setup(&first, b"first").await.unwrap();
    let later = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&later, b"later").await.unwrap();
    storage
        .replace(&ObjectPath::positions(DeviceId(32)), b"positions")
        .await
        .unwrap();
    assert_eq!(storage.setup(&first, b"first").await.unwrap(), config());
    assert_eq!(
        storage
            .setup(&first, b"different")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
    let other = ObjectPath::store_log(DeviceId(32), std::num::NonZeroU64::MIN);
    assert_eq!(
        storage.setup(&other, b"other").await.unwrap_err().failure(),
        StorageSetupFailure::LocationOccupied
    );
    assert_eq!(storage.read(&first).await.unwrap(), b"first");
    assert_eq!(storage.read(&later).await.unwrap(), b"later");
}

#[tokio::test]
async fn listing_records_publication_time_and_keeps_it_on_retry() {
    use coven_foundation::{clock::FixedClock, id_source::DeviceId};
    use std::time::{Duration, SystemTime};
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let published = started + Duration::from_secs(20);
    let clock = Arc::new(FixedClock::new(started));
    let storage = MemoryStorage::new(config(), clock.clone()).unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    clock.set(published);
    storage.finish_upload(&mut upload).await.unwrap();
    clock.set(published + Duration::from_secs(30));
    storage.create_once(&path, b"data").await.unwrap();
    assert_eq!(
        storage.list(&ObjectPrefix::all()).await.unwrap(),
        [StoredObject {
            path,
            size: 4,
            stored_at: published
        }]
    );
    let positions = ObjectPath::positions(DeviceId(31));
    storage.replace(&positions, b"old").await.unwrap();
    clock.set(published + Duration::from_secs(40));
    storage.replace(&positions, b"new position").await.unwrap();
    let listed = storage.list(&ObjectPrefix::positions()).await.unwrap();
    assert_eq!(listed[0].stored_at, published + Duration::from_secs(40));
    assert_eq!(listed[0].size, 12);
}

#[tokio::test]
async fn recorded_sessions_cannot_redirect_or_regress_provider_state() {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 8).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let mut wrong_path = upload.clone();
    wrong_path.path = ObjectPath::device_log(DeviceId(32), std::num::NonZeroU64::MIN);
    let mut wrong_size = upload.clone();
    wrong_size.total = 12;
    for mut invalid in [wrong_path, wrong_size] {
        assert!(matches!(
            storage.resume_upload(&mut invalid).await,
            Err(StorageError::SessionMismatch)
        ));
        assert!(matches!(
            storage.upload_part(&mut invalid, b"next").await,
            Err(StorageError::SessionMismatch)
        ));
        assert!(matches!(
            storage.finish_upload(&mut invalid).await,
            Err(StorageError::SessionMismatch)
        ));
        assert!(matches!(
            storage.abort_upload(&invalid).await,
            Err(StorageError::SessionMismatch)
        ));
    }
    let SessionState::Memory { id, .. } = upload.state else {
        panic!()
    };
    storage
        .state
        .lock()
        .await
        .uploads
        .get_mut(&id)
        .unwrap()
        .bytes
        .clear();
    assert_eq!(
        storage
            .resume_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
    assert_eq!(upload.confirmed_bytes(), 4);
}

#[tokio::test]
async fn publication_removes_pending_parts_and_verifies_the_exact_session() {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let mut first = storage.begin_upload(&path, 4).await.unwrap();
    let mut other = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut first, b"data").await.unwrap();
    storage.upload_part(&mut other, b"data").await.unwrap();
    let mut recorded = first.clone();
    storage.finish_upload(&mut first).await.unwrap();
    let SessionState::Memory { id, .. } = first.state else {
        panic!()
    };
    assert!(!storage.state.lock().await.uploads.contains_key(&id));
    storage.resume_upload(&mut recorded).await.unwrap();
    assert!(recorded.is_complete());
    storage.finish_upload(&mut recorded).await.unwrap();
    assert!(matches!(
        storage.finish_upload(&mut other).await,
        Err(StorageError::AlreadyExists)
    ));
    storage.abort_upload(&other).await.unwrap();
    storage.abort_upload(&other).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut other).await,
        Err(StorageError::AlreadyExists)
    ));
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert!(storage.state.lock().await.uploads.is_empty());
}

#[tokio::test]
async fn s3_fake_requires_the_members_console_key_for_revocation() {
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await,
        Err(StorageError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn the_same_fake_uses_committed_replacement_tokens() {
    use coven_crypto::SecretText;
    use coven_foundation::{clock::FixedClock, id_source::DeviceId};
    use std::time::{Duration, SystemTime};
    let storage = MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "store".into(),
        },
        Arc::new(FixedClock::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&path, b"data").await.unwrap();
    storage
        .set_oauth_tokens(OAuthTokens {
            access_token: SecretText::new("expired".into()),
            refresh_token: None,
            expires_at: Some(SystemTime::UNIX_EPOCH),
        })
        .await
        .unwrap();
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::Authentication
    );
    storage
        .set_oauth_tokens(OAuthTokens {
            access_token: SecretText::new("refreshed".into()),
            refresh_token: None,
            expires_at: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(20)),
        })
        .await
        .unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn lost_publication_replies_and_expired_parts_are_distinct() {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    assert_eq!(
        storage
            .finish_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert!(!upload.is_complete());
    assert!(storage.state.lock().await.uploads.is_empty());
    storage.resume_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    storage.delete(&path).await.unwrap();
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage
        .set_faults(Faults {
            expire_uploads: true,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        storage.resume_upload(&mut upload).await,
        Err(StorageError::SessionExpired)
    ));
    let mut fresh = storage.restart_upload(&upload).await.unwrap();
    storage.upload_part(&mut fresh, b"data").await.unwrap();
    storage.finish_upload(&mut fresh).await.unwrap();
}

fn sharing_configs() -> [StorageConfig; 4] {
    [
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        },
        StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "folder".into(),
        },
        StorageConfig::CloudKit {
            container: "container".into(),
            owner: "owner".into(),
            zone: "zone".into(),
        },
    ]
}

#[tokio::test]
async fn recipient_access_follows_account_grants_and_acceptance() {
    for config in sharing_configs() {
        let owner = MemoryStorage::new(
            config,
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap();
        let path = ObjectPath::store_log(
            coven_foundation::id_source::DeviceId(1),
            std::num::NonZeroU64::MIN,
        );
        owner.create(&path, b"first").await.unwrap();
        let member = MemoryStorage::for_recipient(&owner, "member").unwrap();
        assert_eq!(
            member.read(&path).await.unwrap_err().failure(),
            StorageFailure::PermissionDenied
        );
        let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap()
        else {
            panic!()
        };
        if owner.config().provider() != CloudProvider::GoogleDrive {
            assert_eq!(
                member.read(&path).await.unwrap_err().failure(),
                StorageFailure::PermissionDenied
            );
        }
        member
            .join(&StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap())
            .await
            .unwrap();
        member.join(&invitation).await.unwrap();
        if owner.config().provider() == CloudProvider::OneDrive {
            member
                .join(&StorageInvitation::for_account(owner.config()).unwrap())
                .await
                .unwrap();
        }
        assert_eq!(member.read(&path).await.unwrap(), b"first");
        let kept = MemoryStorage::for_recipient(&owner, "kept").unwrap();
        let AccessGrant::Granted {
            invitation: kept_invite,
        } = owner.grant_access("kept").await.unwrap()
        else {
            panic!()
        };
        kept.join(&kept_invite).await.unwrap();
        let upload_path = ObjectPath::device_log(
            coven_foundation::id_source::DeviceId(2),
            std::num::NonZeroU64::MIN,
        );
        let mut upload = member.begin_upload(&upload_path, 4).await.unwrap();
        owner
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await
            .unwrap();
        for error in [
            member.read(&path).await.unwrap_err(),
            member.list(&ObjectPrefix::all()).await.unwrap_err(),
            member.create(&upload_path, b"data").await.unwrap_err(),
            member.delete(&path).await.unwrap_err(),
            member.upload_part(&mut upload, b"data").await.unwrap_err(),
            member.abort_upload(&upload).await.unwrap_err(),
            member.join(&invitation).await.unwrap_err(),
        ] {
            assert_eq!(error.failure(), StorageFailure::PermissionDenied);
        }
        assert_eq!(owner.read(&path).await.unwrap(), b"first");
        assert_eq!(kept.read(&path).await.unwrap(), b"first");
        let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap()
        else {
            panic!()
        };
        member.join(&invitation).await.unwrap();
        member.upload_part(&mut upload, b"data").await.unwrap();
        member.finish_upload(&mut upload).await.unwrap();
        assert_eq!(owner.read(&upload_path).await.unwrap(), b"data");
    }
}

#[tokio::test]
async fn recipients_keep_their_own_tokens_while_clones_share_the_same_sign_in() {
    let owner = MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "store".into(),
        },
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap() else {
        panic!()
    };
    let member = MemoryStorage::for_recipient(&owner, "member").unwrap();
    member.join(&invitation).await.unwrap();
    let cloned = member.clone();
    let another_sign_in = MemoryStorage::for_recipient(&owner, "member").unwrap();
    member
        .set_oauth_tokens(OAuthTokens {
            access_token: coven_crypto::SecretText::new("expired".into()),
            refresh_token: None,
            expires_at: Some(std::time::SystemTime::UNIX_EPOCH),
        })
        .await
        .unwrap();
    assert_eq!(
        cloned
            .list(&ObjectPrefix::all())
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Authentication
    );
    owner.list(&ObjectPrefix::all()).await.unwrap();
    another_sign_in.list(&ObjectPrefix::all()).await.unwrap();
    member
        .set_oauth_tokens(OAuthTokens {
            access_token: coven_crypto::SecretText::new("refreshed".into()),
            refresh_token: None,
            expires_at: None,
        })
        .await
        .unwrap();
    cloned.list(&ObjectPrefix::all()).await.unwrap();
}

#[tokio::test]
async fn invalid_account_grants_leave_the_fake_unchanged() {
    for config in sharing_configs() {
        let owner = MemoryStorage::new(
            config,
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap();
        assert!(matches!(
            owner.grant_access("").await,
            Err(StorageError::InvalidConfiguration(_))
        ));
        assert!(owner.state.lock().await.accounts.is_empty());
        assert!(MemoryStorage::for_recipient(&owner, "").is_err());
    }
}

#[tokio::test]
async fn a_candidate_connection_does_not_replace_the_active_connections_tokens() {
    use crate::providers::StorageConnector;
    use coven_foundation::{clock::FixedClock, id_source::DeviceId};
    let storage = MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "store".into(),
        },
        Arc::new(FixedClock::new(std::time::UNIX_EPOCH)),
    )
    .unwrap();
    let tokens = |expires_at| {
        StorageCredentials::OAuth(OAuthTokens {
            access_token: coven_crypto::SecretText::new("token".into()),
            refresh_token: None,
            expires_at,
        })
    };
    let active = storage
        .connect(storage.config(), tokens(None), DeviceId(1))
        .await
        .unwrap();
    let candidate = storage
        .connect(
            storage.config(),
            tokens(Some(std::time::UNIX_EPOCH)),
            DeviceId(1),
        )
        .await
        .unwrap();
    assert_eq!(
        candidate
            .list(&ObjectPrefix::all())
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Authentication
    );
    active.list(&ObjectPrefix::all()).await.unwrap();
}
