use super::*;

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
    assert!(matches!(error, StorageError::AccountIdUnavailable));
    assert_eq!(error.failure(), StorageFailure::Refused);
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
