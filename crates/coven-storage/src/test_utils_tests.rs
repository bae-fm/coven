use super::*;
use coven_foundation::clock::FixedClock;
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
    for config in std::iter::once(config()).chain(sharing_configs()) {
        let clock = Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::UNIX_EPOCH,
        ));
        let storage = Arc::new(
            MemoryStorage::builder()
                .location(config)
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let suite = Conformance::new(storage.clone());
        suite.run().await.unwrap();
        suite
            .listing_times(std::time::UNIX_EPOCH, async {
                clock.set(std::time::UNIX_EPOCH + Duration::from_secs(10));
            })
            .await;
        suite
            .expired_session(async {
                storage
                    .set_faults(Faults {
                        expire_uploads: true,
                        ..Faults::none()
                    })
                    .await;
            })
            .await;
        suite
            .lost_part_reply(4, async {
                storage
                    .set_faults(Faults {
                        lose_part_reply: true,
                        ..Faults::none()
                    })
                    .await;
            })
            .await;
        suite
            .permission_failures(async {
                storage
                    .set_faults(Faults {
                        fail_next: usize::MAX,
                        failure: StorageFailure::PermissionDenied,
                        ..Faults::none()
                    })
                    .await;
            })
            .await;
    }
}
#[tokio::test]
async fn crash_after_accepted_part_and_dropped_part_resumes() {
    let provider = MemoryStorage::builder().location(config()).build().unwrap();
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
    assert!(
        matches!(provider.upload_part(&mut session, b"efgh").await, Err(error) if error.failure() == StorageFailure::Network)
    );
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
    assert!(
        matches!(reopened.upload_part(&mut session, b"ij").await, Err(error) if error.failure() == StorageFailure::Network)
    );
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
    let provider = MemoryStorage::builder().location(config()).build().unwrap();
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
    assert!(matches!(
        provider.setup(&path, b"other store").await.unwrap_err(),
        StorageSetupError::LocationOccupied
    ));
}
#[tokio::test]
async fn faults_are_counted_and_delay_is_awaited() {
    use std::{future::Future, task::Poll, time::SystemTime};
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(
        SystemTime::UNIX_EPOCH,
    ));
    let provider = MemoryStorage::builder()
        .location(config())
        .clock(clock.clone())
        .build()
        .unwrap();
    provider
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::QuotaExceeded,
            delay: Duration::from_secs(3),
            ..Faults::none()
        })
        .await;
    let prefix = ObjectPrefix::all();
    for attempt in 0..3 {
        let mut request = Box::pin(provider.list(&prefix));
        std::future::poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(3 * attempt + 2));
        std::future::poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(3 * attempt + 3));
        let result = std::future::poll_fn(|cx| {
            let Poll::Ready(result) = request.as_mut().poll(cx) else {
                panic!("request delay ignored the injected clock")
            };
            Poll::Ready(result)
        })
        .await;
        if attempt < 2 {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::QuotaExceeded);
        } else {
            assert!(result.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn failed_automatic_upload_is_aborted_without_publishing() {
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
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
    assert!(
        !storage.reached(),
        "inner upload calls must not mask its network failure"
    );
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    assert!(storage.provider.state.lock().await.uploads.is_empty());
    assert!(storage.reached());
    storage.reset_reachability();
    assert!(!storage.reached());
    storage.create(&path, &[1; 17]).await.unwrap();
    assert!(storage.reached());
    assert_eq!(storage.read(&path).await.unwrap(), [1; 17]);
}

#[tokio::test]
async fn sharing_authority_belongs_to_the_adapters_account() {
    for config in sharing_configs() {
        let owner = MemoryStorage::builder().location(config).build().unwrap();
        owner.grant_access("kept@example.test").await.unwrap();
        let recipient = MemoryStorage::for_recipient(&owner, "kept@example.test").unwrap();
        Conformance::new(Arc::new(recipient))
            .owner_only_sharing()
            .await;
        assert_eq!(
            owner
                .provider
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
        assert!(owner.provider.state.lock().await.accounts.is_empty());
    }
    let s3 = MemoryStorage::builder().location(config()).build().unwrap();
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
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
    let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.setup(&first, b"first").await.unwrap();
    let later = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&later, b"later").await.unwrap();
    storage
        .replace(&ObjectPath::positions(DeviceId(32)), b"positions")
        .await
        .unwrap();
    assert_eq!(storage.setup(&first, b"first").await.unwrap(), config());
    assert!(matches!(
        storage.setup(&first, b"different").await.unwrap_err(),
        StorageSetupError::LocationOccupied
    ));
    let other = ObjectPath::store_log(DeviceId(32), std::num::NonZeroU64::MIN);
    assert!(matches!(
        storage.setup(&other, b"other").await.unwrap_err(),
        StorageSetupError::LocationOccupied
    ));
    assert_eq!(storage.read(&first).await.unwrap(), b"first");
    assert_eq!(storage.read(&later).await.unwrap(), b"later");
}

#[tokio::test]
async fn listing_records_publication_time_and_keeps_it_on_retry() {
    use coven_foundation::id_source::DeviceId;
    use std::time::{Duration, SystemTime};
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let published = started + Duration::from_secs(20);
    let clock = Arc::new(FixedClock::new(started));
    let storage = MemoryStorage::builder()
        .location(config())
        .clock(clock.clone())
        .build()
        .unwrap();
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
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
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
            Err(error) if error.failure() == StorageFailure::SessionMismatch
        ));
        assert!(matches!(
            storage.upload_part(&mut invalid, b"next").await,
            Err(error) if error.failure() == StorageFailure::SessionMismatch
        ));
        assert!(matches!(
            storage.finish_upload(&mut invalid).await,
            Err(error) if error.failure() == StorageFailure::SessionMismatch
        ));
        assert!(matches!(
            storage.abort_upload(&invalid).await,
            Err(error) if error.failure() == StorageFailure::SessionMismatch
        ));
    }
    let SessionState::Memory { id, .. } = upload.state else {
        panic!()
    };
    storage
        .provider
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
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
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
    assert!(!storage
        .provider
        .state
        .lock()
        .await
        .uploads
        .contains_key(&id));
    storage.resume_upload(&mut recorded).await.unwrap();
    assert!(recorded.is_complete());
    storage.finish_upload(&mut recorded).await.unwrap();
    assert!(matches!(
        storage.finish_upload(&mut other).await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    storage.abort_upload(&other).await.unwrap();
    storage.abort_upload(&other).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut other).await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert!(storage.provider.state.lock().await.uploads.is_empty());
}

#[tokio::test]
async fn s3_fake_requires_the_members_console_key_for_revocation() {
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await,
        Err(error) if error.failure() == StorageFailure::InvalidConfiguration
    ));
}

#[tokio::test]
async fn the_same_fake_uses_committed_replacement_tokens() {
    use coven_crypto::SecretText;
    use coven_foundation::id_source::DeviceId;
    use std::time::{Duration, SystemTime};
    let storage = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .clock(Arc::new(FixedClock::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        )))
        .build()
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
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
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
    assert!(storage.provider.state.lock().await.uploads.is_empty());
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
        Err(error) if error.failure() == StorageFailure::SessionExpired
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
        let owner = MemoryStorage::builder().location(config).build().unwrap();
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
    let owner = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .build()
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
        let owner = MemoryStorage::builder().location(config).build().unwrap();
        assert!(matches!(
            owner.grant_access("").await,
            Err(error) if error.failure() == StorageFailure::InvalidConfiguration
        ));
        assert!(owner.provider.state.lock().await.accounts.is_empty());
        assert!(
            matches!(MemoryStorage::for_recipient(&owner, ""), Err(error) if error.failure() == StorageFailure::InvalidConfiguration)
        );
    }
}

#[tokio::test]
async fn a_candidate_connection_does_not_replace_the_active_connections_tokens() {
    use crate::providers::StorageConnector;
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .build()
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
