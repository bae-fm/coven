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
    let shares = storage.revoke_access("target@example.test").await.unwrap();
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
    storage.revoke_access("target@example.test").await.unwrap();
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
    let shares = storage.revoke_access("owner@example.test").await.unwrap();
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
    let member = "target@example.test";
    assert_eq!(
        storage.revoke_access(member).await.err().unwrap().failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().permissions.contains_key("a-invited"));
    assert!(!state.lock().unwrap().permissions.contains_key("z-native"));
    assert!(storage.revoke_access(member).await.unwrap().is_empty());
    assert!(storage.revoke_access(member).await.unwrap().is_empty());
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
    let shares = storage.revoke_access("target@example.test").await.unwrap();
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
                .revoke_access("target@example.test")
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
async fn sharing_with_the_store_owners_account_is_refused_before_any_change() {
    let state = remote();
    state.lock().unwrap().permissions.insert("owner".into(), json!({"id":"owner", "roles":["owner"], "grantedToV2":{"siteUser":{"email":"owner@example.test"}}}));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .grant_access("OWNER@example.test")
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::InviteeIsStoreOwner
    );
    let state = state.lock().unwrap();
    assert!(state.members.is_empty());
    assert_eq!(state.permissions["owner"]["roles"], json!(["owner"]));
    assert!(state
        .requests
        .iter()
        .all(|(method, uri)| *method == Method::GET && !uri.contains("/invite")));
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
    assert!(storage
        .revoke_access("target@example.test")
        .await
        .unwrap()
        .is_empty());
    assert!(state.lock().unwrap().permissions.is_empty());
}

#[tokio::test]
async fn invitation_uses_encoded_sharing_url_when_permission_id_is_not_a_shares_key() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            captured.lock().unwrap().push((
                method.clone(),
                uri.path().to_owned(),
                headers.get("prefer").cloned(),
            ));
            async move {
                assert_eq!(method, Method::GET);
                assert!(body.is_empty());
                match uri.path() {
                    "/graph/me/drive" => reply(json!({"id":"drive"})),
                    "/graph/me" => reply(json!({"mail":"owner@example.test"})),
                    "/graph/drives/drive/items/root/permissions" => reply(json!({"value":[{
                        "id":"permission-id", "shareId":"permission-id", "roles":["write"],
                        "grantedToV2":{"siteUser":{"email":"member@example.test"}},
                        "link":{"webUrl":"https://1drv.ms/f/example?e=abc"}
                    }]})),
                    "/graph/shares/u!aHR0cHM6Ly8xZHJ2Lm1zL2YvZXhhbXBsZT9lPWFiYw/driveItem" => {
                        reply(
                            json!({"id":"root","folder":{},"parentReference":{"driveId":"drive"}}),
                        )
                    }
                    _ => response(
                        400,
                        json!({"error":{"code":"invalidRequest","message":"Invalid shares key."}})
                            .to_string(),
                    ),
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let AccessGrant::Granted { invitation } =
        storage.grant_access("member@example.test").await.unwrap()
    else {
        panic!()
    };
    let invitation = StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap();
    storage.provider.join(&invitation).await.unwrap();
    let requests = requests.lock().unwrap();
    let redemption: Vec<_> = requests
        .iter()
        .filter_map(|(_, _, prefer)| prefer.as_ref().map(|p| p.to_str().unwrap()))
        .collect();
    assert_eq!(
        redemption,
        ["redeemSharingLinkIfNecessary", "redeemSharingLink"]
    );
    assert!(requests
        .iter()
        .all(|(_, path, _)| !path.contains("/shares/permission-id/")));
}
