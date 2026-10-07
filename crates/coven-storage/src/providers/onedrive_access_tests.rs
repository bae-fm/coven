use super::*;

#[tokio::test]
async fn revocation_preserves_permissions_that_also_reach_other_accounts() {
    let state = remote();
    {
        let mut remote = state.lock().unwrap();
        remote.members.insert("target@example.test".into());
        remote.permissions.insert("shared".into(), json!({"id":"shared", "roles":["write"], "link":{"scope":"users"}, "grantedToIdentitiesV2":[{"user":{"email":"target@example.test"}}, {"user":{"email":"kept@example.test"}}]}));
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let MemberRemoval::AccessRemains { shares } = storage
        .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
        .await
        .unwrap()
    else {
        panic!("shared permission was not reported")
    };
    assert_eq!(
        shares,
        [RetainedAccess {
            provider_id: "shared".into(),
            reason: RetainedAccessReason::OtherAccounts,
        }]
    );
    let remote = state.lock().unwrap();
    assert!(remote.permissions.contains_key("shared"));
    assert!(!remote.members.contains("target@example.test"));
    assert_eq!(remote.deleted_permissions, ["target@example.test"]);
}

#[tokio::test]
async fn revocation_resolves_native_account_ids_before_deleting_the_email() {
    let state = remote();
    {
        let mut remote = state.lock().unwrap();
        remote.permissions.insert("a-invited".into(), json!({"id":"a-invited", "roles":["write"], "invitation":{"email":"target@example.test"}, "grantedToV2":{"user":{"id":"account"}, "siteUser":{"id":"site-account"}}}));
        remote.permissions.insert("z-native".into(), json!({"id":"z-native", "roles":["write"], "grantedToV2":{"siteUser":{"id":"site-account"}}}));
        remote.permissions.insert("other".into(), json!({"id":"other", "roles":["write"], "grantedToV2":{"user":{"email":"other@example.test"}}}));
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage
        .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
        .await
        .unwrap();
    let remote = state.lock().unwrap();
    assert_eq!(
        remote.permissions.keys().cloned().collect::<Vec<_>>(),
        ["other"]
    );
    assert_eq!(remote.deleted_permissions, ["z-native", "a-invited"]);
}

#[tokio::test]
async fn revocation_never_removes_the_store_owners_permission() {
    let state = remote();
    state.lock().unwrap().permissions.insert("owner".into(), json!({"id":"owner", "roles":["owner"], "grantedToV2":{"user":{"email":"owner@example.test"}}}));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let MemberRemoval::AccessRemains { shares } = storage
        .revoke_access(&MemberAccess::ProviderAccount("owner@example.test".into()))
        .await
        .unwrap()
    else {
        panic!("owner permission was not reported")
    };
    assert_eq!(
        shares,
        [RetainedAccess {
            provider_id: "owner".into(),
            reason: RetainedAccessReason::StoreOwner,
        }]
    );
    assert!(state.lock().unwrap().permissions.contains_key("owner"));
    assert!(state.lock().unwrap().deleted_permissions.is_empty());
}

#[tokio::test]
async fn revocation_keeps_account_identity_available_after_a_lost_delete_reply() {
    let state = remote();
    {
        let mut remote = state.lock().unwrap();
        remote.permissions.insert("a-invited".into(), json!({"id":"a-invited", "roles":["write"], "invitation":{"email":"target@example.test"}, "grantedToV2":{"user":{"id":"account"}}}));
        remote.permissions.insert(
            "z-native".into(),
            json!({"id":"z-native", "roles":["write"], "grantedToV2":{"user":{"id":"account"}}}),
        );
        remote.lose_permission_reply = true;
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let member = MemberAccess::ProviderAccount("target@example.test".into());
    assert_eq!(
        storage
            .revoke_access(&member)
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().permissions.contains_key("a-invited"));
    assert!(!state.lock().unwrap().permissions.contains_key("z-native"));
    assert!(matches!(
        storage.revoke_access(&member).await.unwrap(),
        MemberRemoval::Revoked
    ));
    assert!(matches!(
        storage.revoke_access(&member).await.unwrap(),
        MemberRemoval::Revoked
    ));
    assert_eq!(
        state.lock().unwrap().deleted_permissions,
        ["z-native", "a-invited"]
    );
}

#[tokio::test]
async fn revocation_reports_broad_inherited_and_unidentified_access() {
    let state = remote();
    let permissions = [
        json!({"id":"anonymous", "roles":["read"], "link":{"scope":"anonymous"}}),
        json!({"id":"inherited", "roles":["read"], "invitation":{"email":"target@example.test"}, "inheritedFrom":{"id":"parent"}}),
        json!({"id":"organization", "roles":["write"], "link":{"scope":"organization"}}),
        json!({"id":"unknown", "roles":["write"], "grantedToV2":{"user":{"id":"unresolved"}}}),
        json!({"id":"existing", "roles":["read"], "link":{"scope":"existingAccess"}}),
        json!({"id":"group", "roles":["write"], "grantedToV2":{"group":{"id":"group-id"}}}),
    ];
    for permission in &permissions {
        state.lock().unwrap().permissions.insert(
            permission["id"].as_str().unwrap().into(),
            permission.clone(),
        );
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let MemberRemoval::AccessRemains { shares } = storage
        .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
        .await
        .unwrap()
    else {
        panic!("remaining access was hidden")
    };
    assert_eq!(
        shares,
        [
            RetainedAccess {
                provider_id: "anonymous".into(),
                reason: RetainedAccessReason::OtherAccounts
            },
            RetainedAccess {
                provider_id: "group".into(),
                reason: RetainedAccessReason::UnidentifiedAccount
            },
            RetainedAccess {
                provider_id: "inherited".into(),
                reason: RetainedAccessReason::Inherited
            },
            RetainedAccess {
                provider_id: "organization".into(),
                reason: RetainedAccessReason::OtherAccounts
            },
            RetainedAccess {
                provider_id: "unknown".into(),
                reason: RetainedAccessReason::UnidentifiedAccount
            },
        ]
    );
    assert_eq!(state.lock().unwrap().permissions.len(), permissions.len());
    assert!(state.lock().unwrap().deleted_permissions.is_empty());
}

#[tokio::test]
async fn revocation_validates_every_page_before_mutating_permissions() {
    let state = remote();
    state
        .lock()
        .unwrap()
        .members
        .insert("target@example.test".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for invalid in [
        json!({"id":"bad", "roles":["write"], "grantedToIdentitiesV2":[17]}),
        json!({"id":"bad", "roles":[], "invitation":{"email":"target@example.test"}}),
        json!({"id":"bad", "roles":["write"], "link":{"scope":17}}),
    ] {
        state
            .lock()
            .unwrap()
            .permissions
            .insert("bad".into(), invalid);
        assert_eq!(
            storage
                .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
                .await
                .err()
                .unwrap()
                .failure(),
            StorageFailure::Protocol
        );
        assert!(state
            .lock()
            .unwrap()
            .members
            .contains("target@example.test"));
        assert!(state.lock().unwrap().deleted_permissions.is_empty());
    }
}

#[tokio::test]
async fn sharing_an_owner_does_not_replace_their_permission() {
    let state = remote();
    state.lock().unwrap().permissions.insert("owner".into(), json!({"id":"owner", "roles":["owner"], "grantedToV2":{"siteUser":{"email":"owner@example.test"}}}));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("OWNER@example.test").await.unwrap();
    assert!(state.lock().unwrap().members.is_empty());
    assert_eq!(
        state.lock().unwrap().permissions["owner"]["roles"],
        json!(["owner"])
    );
}

#[tokio::test]
async fn native_identity_fields_identify_the_account_without_an_invitation() {
    let state = remote();
    for (id, field, identity) in [
        (
            "v2",
            "grantedToV2",
            json!({"siteUser":{"email":"TARGET@example.test"}}),
        ),
        (
            "native",
            "grantedTo",
            json!({"user":{"email":"target@example.test"}}),
        ),
    ] {
        let mut permission = json!({"id":id, "roles":["write"]});
        permission[field] = identity;
        state
            .lock()
            .unwrap()
            .permissions
            .insert(id.into(), permission);
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert!(matches!(
        storage
            .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
            .await
            .unwrap(),
        MemberRemoval::Revoked
    ));
    assert!(state.lock().unwrap().permissions.is_empty());
}
