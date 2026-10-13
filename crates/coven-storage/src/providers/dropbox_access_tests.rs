use super::*;
use crate::test_utils::STORE_ID;

#[tokio::test]
async fn account_removal_preserves_parent_and_group_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    {
        let mut remote = state.lock().unwrap();
        remote.members.insert("member".into(), "editor".into());
        remote.members.insert("other".into(), "editor".into());
        remote
            .inherited_members
            .insert("member".into(), "viewer".into());
        remote
            .groups
            .push(json!({"group":{"group_id":"group-1"},"access_type":{".tag":"editor"}}));
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let MemberRemoval::AccessRemains { shares } = storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap()
    else {
        panic!("parent and group access hidden")
    };
    assert!(shares.iter().any(|share| share.provider_id == "group-1"
        && share.reason == RetainedAccessReason::OtherAccounts));
    assert!(shares
        .iter()
        .any(|share| share.reason == RetainedAccessReason::Inherited));
    let MemberRemoval::AccessRemains { .. } = storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap()
    else {
        panic!("retry hid remaining access")
    };
    let remote = state.lock().unwrap();
    assert!(!remote.members.contains_key("member"));
    assert!(remote.members.contains_key("other"));
    assert!(remote.inherited_members.contains_key("member"));
    assert_eq!(remote.groups.len(), 1);
    assert_eq!(remote.sharing_mutations, ["remove"]);
}

#[tokio::test]
async fn account_removal_leaves_the_store_owner() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .members
        .insert("owner".into(), "owner".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let MemberRemoval::AccessRemains { shares } = storage
        .revoke_access(&MemberAccess::ProviderAccount("owner".into()))
        .await
        .unwrap()
    else {
        panic!("owner must be retained")
    };
    assert_eq!(
        shares,
        [RetainedAccess {
            provider_id: "dbid:owner".into(),
            reason: RetainedAccessReason::StoreOwner
        }]
    );
    assert!(state.lock().unwrap().sharing_mutations.is_empty());
}

#[tokio::test]
async fn pending_invite_matches_the_invited_alias_and_removes_only_that_account() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state.lock().unwrap().invitees.push(json!({"invitee":{".tag":"email","email":"alias"},"user":{"account_id":"dbid:canonical","email":"canonical"},"access_type":{".tag":"editor"}}));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("canonical".into()))
            .await
            .unwrap(),
        MemberRemoval::Revoked
    ));
    assert!(state.lock().unwrap().invitees.is_empty());
    assert_eq!(state.lock().unwrap().sharing_mutations, ["remove"]);
}

#[tokio::test]
async fn lost_removal_reply_can_be_retried_without_changing_other_accounts() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .members
        .insert("member".into(), "editor".into());
    state.lock().unwrap().lose_remove_reply = true;
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::Network
    );
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await
            .unwrap(),
        MemberRemoval::Revoked
    ));
    assert_eq!(state.lock().unwrap().sharing_mutations, ["remove"]);
}

#[tokio::test]
async fn a_pending_viewer_without_an_account_id_keeps_their_invitation() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let invite =
        json!({"invitee":{".tag":"email","email":"member"},"access_type":{".tag":"viewer"}});
    state.lock().unwrap().invitees.push(invite.clone());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let error = storage.grant_access("member").await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::AccountIdUnavailable);
    assert_eq!(state.lock().unwrap().invitees, [invite]);
    assert!(state.lock().unwrap().sharing_mutations.is_empty());
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await
            .unwrap(),
        MemberRemoval::Revoked
    ));
    assert!(state.lock().unwrap().invitees.is_empty());
}

#[tokio::test]
async fn sharing_requires_the_store_owners_account() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("kept@example.test").await.unwrap();
    state.lock().unwrap().non_owner = true;
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .owner_only_sharing()
        .await;
    assert_eq!(
        state
            .lock()
            .unwrap()
            .members
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["kept@example.test"]
    );
}

#[tokio::test]
async fn viewer_upgrade_preserves_access_when_the_provider_refuses_it() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .members
        .insert("member@example.test".into(), "viewer".into());
    state.lock().unwrap().refuse_share = true;
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .grant_access("member@example.test")
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .members
            .get("member@example.test")
            .map(String::as_str),
        Some("viewer")
    );
    assert_eq!(state.lock().unwrap().sharing_mutations, ["update"]);
    state.lock().unwrap().refuse_share = false;
    storage.grant_access("member@example.test").await.unwrap();
    assert_eq!(
        state.lock().unwrap().members["member@example.test"],
        "editor"
    );
    assert_eq!(
        state.lock().unwrap().sharing_mutations,
        ["update", "update"]
    );
}

#[tokio::test]
async fn viewer_upgrade_retries_a_lost_reply_without_removing_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .members
        .insert("member@example.test".into(), "viewer_no_comment".into());
    state.lock().unwrap().lose_update_reply = true;
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .grant_access("member@example.test")
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::Network
    );
    storage.grant_access("member@example.test").await.unwrap();
    assert_eq!(
        state.lock().unwrap().members["member@example.test"],
        "editor"
    );
    assert_eq!(state.lock().unwrap().sharing_mutations, ["update"]);
}

#[tokio::test]
async fn granting_an_owner_does_not_downgrade_their_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .members
        .insert("owner@example.test".into(), "owner".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("owner@example.test").await.unwrap();
    assert_eq!(state.lock().unwrap().members["owner@example.test"], "owner");
    assert!(state.lock().unwrap().sharing_mutations.is_empty());
}

mod app_folder_tests {
    use super::*;

    async fn app_folder(url: &str) -> StorageConnection<DropboxStorage> {
        let session = session(PROVIDER);
        session
            .set_tokens(OAuthTokens {
                client_id: "folder-key".into(),
                access: OAuthAccess::Dropbox(DropboxAccess::AppFolder),
                access_token: SecretText::new("token".into()),
                refresh_token: None,
                expires_at: None,
            })
            .await;
        let mut provider = DropboxStorage::new(
            StorageConfig::Dropbox {
                location: DropboxLocation::AppFolder,
            },
            session,
        )
        .unwrap();
        provider.api = format!("{url}/2");
        provider.content = provider.api.clone();
        StorageConnection::from_provider(Arc::new(provider))
    }

    #[tokio::test]
    async fn app_folder_objects_and_sessions_use_relative_paths_without_namespace_headers() {
        let state = Arc::new(Mutex::new(Remote {
            app_folder: true,
            ..Remote::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = Arc::new(app_folder(&server.url).await);
        crate::test_utils::Conformance::new(storage.clone(), STORE_ID)
            .run()
            .await
            .unwrap();
        let path = ObjectPath::device_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        assert!(storage.status(&path).await.unwrap().is_none());
        let mut upload = storage.begin_upload(&path, 4).await.unwrap();
        storage.upload_part(&mut upload, b"data").await.unwrap();
        let mut upload = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(&mut upload).await.unwrap();
        storage.finish_upload(&mut upload).await.unwrap();
        assert_eq!(storage.read(&path).await.unwrap(), b"data");
        assert_eq!(storage.config(), upload.location);
        assert!(state.lock().unwrap().objects.contains_key(&path.absolute()));
    }

    #[tokio::test]
    async fn app_folder_sharing_fails_before_any_provider_request() {
        let state = Arc::new(Mutex::new(Remote {
            app_folder: true,
            ..Remote::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = app_folder(&server.url).await;
        assert!(
            matches!(storage.grant_access("member@example.test").await, Err(error) if error.failure() == StorageFailure::DropboxAppFolderNotShareable)
        );
        assert!(
            matches!(storage.revoke_access(&MemberAccess::ProviderAccount("member@example.test".into())).await, Err(error) if error.failure() == StorageFailure::DropboxAppFolderNotShareable)
        );
        assert!(
            matches!(StorageInvitation::for_account(storage.config()), Err(error) if error.failure() == StorageFailure::DropboxAppFolderNotShareable)
        );
        assert_eq!(state.lock().unwrap().requests, 0);
    }
}
