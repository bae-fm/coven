use super::*;
use crate::test_utils::STORE_ID;
use coven_foundation::clock::FixedClock;
fn config() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
        prefix: "store".into(),
    }
}
#[tokio::test]
async fn equal_size_and_time_replacements_have_distinct_identity() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"first").await.unwrap();
    let mut first = storage.status(&path).await.unwrap().unwrap();
    for bytes in [b"other", b"other"] {
        storage.replace(&first, bytes).await.unwrap();
        let next = storage.status(&path).await.unwrap().unwrap();
        assert_eq!(first.size, next.size);
        assert_eq!(first.stored_at, next.stored_at);
        assert_ne!(first, next, "replacement identity must include a revision");
        first = next;
    }
}

#[tokio::test]
async fn failed_status_is_not_absence_and_says_whether_storage_answered() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    assert_eq!(storage.status(&path).await.unwrap(), None);
    for response in [None, Some(503)] {
        storage
            .set_faults(Faults {
                fail_next: 1,
                failure: StorageFailure::Network,
                response,
                ..Faults::none()
            })
            .await;
        let error = storage.status(&path).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Network);
        assert_eq!(error.answered(), response.is_some());
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
                .transfer_limits(16, 4)
                .location(config)
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let suite = Conformance::new(storage.clone(), STORE_ID);
        suite.run().await.unwrap();
        let claimant: Arc<dyn Storage> = storage.clone();
        suite
            .creation_race([claimant.clone(), claimant.clone(), claimant])
            .await
            .unwrap();
        suite
            .shared_location(coven_foundation::id_source::StoreId(
                uuid::Uuid::from_bytes([0x20; 16]),
            ))
            .await;
        suite
            .listing_times(
                coven_format::value::StorageTime::new(0, 0).unwrap(),
                async {
                    clock.set(std::time::UNIX_EPOCH + Duration::from_secs(10));
                },
            )
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
    let provider = MemoryStorage::builder()
        .transfer_limits(16, 4)
        .location(config())
        .build()
        .unwrap();
    let path = ObjectPath::file(
        STORE_ID,
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
    let prefix = ObjectPrefix::all(STORE_ID);
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
    let storage = MemoryStorage::builder()
        .transfer_limits(16, 4)
        .location(config())
        .build()
        .unwrap();
    storage
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let error = storage.create(&path, &[1; 17]).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(
        !error.answered(),
        "inner upload calls must not mask its lost reply"
    );
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert!(storage.provider.state.lock().await.uploads.is_empty());
    storage.create(&path, &[1; 17]).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), [1; 17]);
}

#[tokio::test]
async fn sharing_authority_belongs_to_the_adapters_account() {
    for config in sharing_configs() {
        let owner = MemoryStorage::builder().location(config).build().unwrap();
        owner.grant_access("kept@example.test").await.unwrap();
        let recipient = MemoryStorage::for_recipient(&owner, "kept@example.test").unwrap();
        Conformance::new(Arc::new(recipient), STORE_ID)
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
        owner.revoke_access("kept@example.test").await.unwrap();
        assert!(owner.provider.state.lock().await.accounts.is_empty());
    }
    let s3 = MemoryStorage::builder().location(config()).build().unwrap();
    assert!(matches!(
        s3.grant_access("member").await.unwrap(),
        AccessGrant::CreateAccessKey
    ));
    assert_eq!(
        s3.revoke_access("member").await.unwrap_err().failure(),
        StorageFailure::InvalidConfiguration
    );
}

#[tokio::test]
async fn listing_records_publication_time_and_keeps_it_on_retry() {
    use std::time::{Duration, SystemTime};
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let published = started + Duration::from_secs(20);
    let clock = Arc::new(FixedClock::new(started));
    let storage = MemoryStorage::builder()
        .location(config())
        .clock(clock.clone())
        .build()
        .unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(storage.status(&path).await.unwrap(), None);
    assert_eq!(
        storage
            .read_object(&path, None)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::NotFound
    );
    clock.set(published);
    storage.finish_upload(&mut upload).await.unwrap();
    let initial = storage.status(&path).await.unwrap().unwrap();
    let storage_time = |time| coven_format::value::StorageTime::from_system_time(time).unwrap();
    assert_eq!(initial.stored_at, storage_time(published));
    clock.set(published + Duration::from_secs(30));
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(storage.status(&path).await.unwrap(), Some(initial));
    let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, path);
    assert_eq!(listed[0].size, 4);
    assert_eq!(listed[0].stored_at, storage_time(published));
    let floor = ObjectPath::floor(STORE_ID);
    storage.create(&floor, b"old").await.unwrap();
    let previous = storage.status(&floor).await.unwrap().unwrap();
    clock.set(published + Duration::from_secs(40));
    storage.replace(&previous, b"new floor!!!").await.unwrap();
    let listed = [storage.status(&floor).await.unwrap().unwrap()];
    assert_eq!(
        listed[0].stored_at,
        storage_time(published + Duration::from_secs(40))
    );
    assert_eq!(listed[0].size, 12);
}

#[tokio::test]
async fn recorded_sessions_cannot_redirect_or_regress_provider_state() {
    let storage = MemoryStorage::builder()
        .transfer_limits(16, 4)
        .location(config())
        .build()
        .unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 8).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let mut wrong_path = upload.clone();
    wrong_path.path = ObjectPath::slot(STORE_ID, 2.try_into().unwrap());
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
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
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
async fn s3_fake_refuses_account_revocation() {
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
    assert!(matches!(
        storage
            .revoke_access("member")
            .await,
        Err(error) if error.failure() == StorageFailure::InvalidConfiguration
    ));
}

fn dropbox_tokens(token: &str) -> OAuthTokens {
    OAuthTokens {
        client_id: "dropbox".into(),
        access: crate::OAuthAccess::Dropbox(crate::DropboxAccess::FullDropbox),
        access_token: coven_crypto::SecretText::new(token.into()),
        refresh_token: None,
    }
}

#[tokio::test]
async fn a_rejected_fake_token_refreshes_once_through_custody_and_retries_once() {
    use crate::providers::StorageConnector;
    let storage = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .build()
        .unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"data").await.unwrap();
    let custody = crate::token_custody::tests::CountingCustody::new(false);
    let connection = storage
        .connect(
            storage.config(),
            crate::ProviderAuth::OAuth {
                tokens: dropbox_tokens("token"),
                custody: custody.clone(),
            },
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
        )
        .await
        .unwrap();
    storage.reject_access_token("token").await;
    let before = storage.request_count();
    assert_eq!(connection.read(&path).await.unwrap(), b"data");
    assert_eq!(
        storage.request_count(),
        before + 2,
        "one rejection, one retry"
    );
    connection.read(&path).await.unwrap();
    assert_eq!(custody.count(), 1);
    // A replacement the provider rejects too reaches the caller after one retry.
    storage.reject_access_token("refreshed-0").await;
    storage.reject_access_token("refreshed-1").await;
    assert_eq!(
        connection.read(&path).await.unwrap_err().failure(),
        StorageFailure::Authentication
    );
    assert_eq!(custody.count(), 2);
}

#[tokio::test]
async fn the_same_fake_uses_committed_replacement_tokens() {
    let storage = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .build()
        .unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"data").await.unwrap();
    storage
        .set_oauth_tokens(dropbox_tokens("rejected"))
        .await
        .unwrap();
    storage.reject_access_token("rejected").await;
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::Authentication
    );
    storage
        .set_oauth_tokens(dropbox_tokens("refreshed"))
        .await
        .unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn lost_publication_replies_and_expired_parts_are_distinct() {
    let storage = MemoryStorage::builder().location(config()).build().unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
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

fn sharing_configs() -> [StorageConfig; 3] {
    [
        StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "namespace".into(),
            },
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
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
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
        assert_eq!(
            member.read(&path).await.unwrap_err().failure(),
            StorageFailure::PermissionDenied
        );
        member
            .join(
                STORE_ID,
                &StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap(),
            )
            .await
            .unwrap();
        member.join(STORE_ID, &invitation).await.unwrap();
        if owner.config().provider() == CloudProvider::OneDrive {
            member
                .join(
                    STORE_ID,
                    &StorageInvitation::for_account(owner.config()).unwrap(),
                )
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
        kept.join(STORE_ID, &kept_invite).await.unwrap();
        let upload_path = ObjectPath::slot(STORE_ID, 2.try_into().unwrap());
        let mut upload = member.begin_upload(&upload_path, 4).await.unwrap();
        owner.revoke_access("member").await.unwrap();
        for error in [
            member.read(&path).await.unwrap_err(),
            member.list(&ObjectPrefix::all(STORE_ID)).await.unwrap_err(),
            member.create(&upload_path, b"data").await.unwrap_err(),
            member.delete(&path).await.unwrap_err(),
            member.upload_part(&mut upload, b"data").await.unwrap_err(),
            member.abort_upload(&upload).await.unwrap_err(),
            member.join(STORE_ID, &invitation).await.unwrap_err(),
        ] {
            assert_eq!(error.failure(), StorageFailure::PermissionDenied);
        }
        assert_eq!(owner.read(&path).await.unwrap(), b"first");
        assert_eq!(kept.read(&path).await.unwrap(), b"first");
        let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap()
        else {
            panic!()
        };
        member.join(STORE_ID, &invitation).await.unwrap();
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
    member.join(STORE_ID, &invitation).await.unwrap();
    let cloned = member.clone();
    let another_sign_in = MemoryStorage::for_recipient(&owner, "member").unwrap();
    member
        .set_oauth_tokens(dropbox_tokens("rejected"))
        .await
        .unwrap();
    owner.reject_access_token("rejected").await;
    assert_eq!(
        cloned
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Authentication
    );
    owner.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    another_sign_in
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap();
    member
        .set_oauth_tokens(dropbox_tokens("refreshed"))
        .await
        .unwrap();
    cloned.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
}

#[tokio::test]
async fn invalid_account_grants_leave_the_fake_unchanged() {
    let invalid = sharing_configs()
        .into_iter()
        .flat_map(|config| {
            [
                (config.clone(), "", StorageFailure::InvalidConfiguration),
                (
                    config,
                    "Owner@example.test",
                    StorageFailure::InviteeIsStoreOwner,
                ),
            ]
        })
        .chain([(
            StorageConfig::Dropbox {
                location: crate::DropboxLocation::AppFolder,
            },
            "member",
            StorageFailure::DropboxAppFolderNotShareable,
        )]);
    for (config, account, failure) in invalid {
        let owner = MemoryStorage::builder().location(config).build().unwrap();
        assert!(matches!(
            owner.grant_access(account).await,
            Err(error) if error.failure() == failure
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
    let storage = MemoryStorage::builder()
        .provider(crate::CloudProvider::Dropbox)
        .build()
        .unwrap();
    let tokens = |token: &str| crate::ProviderAuth::OAuth {
        tokens: dropbox_tokens(token),
        custody: Arc::new(crate::test_utils::RefusedRefresh),
    };
    storage.reject_access_token("rejected").await;
    let active = storage
        .connect(
            storage.config(),
            tokens("token"),
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
        )
        .await
        .unwrap();
    let candidate = storage
        .connect(
            storage.config(),
            tokens("rejected"),
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
        )
        .await
        .unwrap();
    assert_eq!(
        candidate
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Authentication
    );
    active.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
}

#[tokio::test]
async fn paged_listings_charge_every_page() {
    let storage = MemoryStorage::builder()
        .listing_page(2.try_into().unwrap())
        .build()
        .unwrap();
    for device in 1..=5 {
        let path = ObjectPath::file(
            STORE_ID,
            coven_foundation::id_source::DeviceId(device),
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(1)),
        );
        storage.create(&path, b"entry").await.unwrap();
    }
    let before = storage.request_count();
    assert_eq!(
        storage
            .list(&ObjectPrefix::files(STORE_ID))
            .await
            .unwrap()
            .len(),
        5
    );
    assert_eq!(storage.request_count() - before, 3);
}

mod sequence {
    use super::*;

    #[tokio::test]
    async fn floor_replacement_conflicts_and_lost_reply_preserve_the_accepted_revision() {
        let storage = MemoryStorage::builder().build().unwrap();
        let path = ObjectPath::floor(crate::test_utils::STORE_ID);
        storage.create(&path, b"floor one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        storage.replace(&first, b"floor two").await.unwrap();
        assert_eq!(
            storage
                .replace(&first, b"stale floor")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        assert_eq!(storage.read(&path).await.unwrap(), b"floor two");
        let second = storage.status(&path).await.unwrap().unwrap();
        storage
            .set_faults(Faults {
                lose_completion_reply: true,
                ..Faults::none()
            })
            .await;
        assert_eq!(
            storage
                .replace(&second, b"floor three")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(
            storage
                .replace(&second, b"stale retry")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        let third = storage.read_object(&path, None).await.unwrap();
        assert_eq!(third.bytes, b"floor three");
        assert_ne!(second.revision, third.metadata.revision);
        assert_ne!(second.replace_token, third.metadata.replace_token);
    }

    #[tokio::test]
    async fn slot_create_once_refuses_equal_and_different_bytes_without_replacement() {
        let storage = MemoryStorage::builder().build().unwrap();
        let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
        storage.create(&path, b"claim one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        for bytes in [b"claim one", b"claim two"] {
            assert_eq!(
                storage.create(&path, bytes).await.unwrap_err().failure(),
                StorageFailure::AlreadyExists
            );
            assert_eq!(storage.read(&path).await.unwrap(), b"claim one");
            assert_eq!(storage.status(&path).await.unwrap().unwrap(), first);
        }
    }
}
