use super::*;

#[tokio::test]
async fn revocation_removes_direct_access_and_reports_inherited_and_broad_grants() {
    let state = Arc::new(Mutex::new(Remote::default()));
    for permission in [
        json!({"id":"target", "type":"user", "emailAddress":"target@example.test", "role":"writer", "permissionDetails":[{"inherited":true},{"inherited":false}]}),
        json!({"id":"group", "type":"group", "emailAddress":"group@example.test", "role":"reader", "permissionDetails":[{"inherited":false}]}),
        json!({"id":"public", "type":"anyone", "role":"reader", "permissionDetails":[{"inherited":false}]}),
        json!({"id":"other", "type":"user", "emailAddress":"other@example.test", "role":"writer", "permissionDetails":[{"inherited":false}]}),
    ] {
        state
            .lock()
            .unwrap()
            .extra_permissions
            .insert(permission["id"].as_str().unwrap().into(), permission);
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let member = MemberAccess::ProviderAccount("target@example.test".into());
    for _ in 0..2 {
        let MemberRemoval::AccessRemains { shares } = storage.revoke_access(&member).await.unwrap()
        else {
            panic!("remaining access was hidden")
        };
        assert_eq!(
            shares,
            [
                RetainedAccess {
                    provider_id: "group".into(),
                    reason: RetainedAccessReason::OtherAccounts
                },
                RetainedAccess {
                    provider_id: "public".into(),
                    reason: RetainedAccessReason::OtherAccounts
                },
                RetainedAccess {
                    provider_id: "target".into(),
                    reason: RetainedAccessReason::Inherited
                },
            ]
        );
    }
    let remote = state.lock().unwrap();
    assert_eq!(
        remote.permission_mutations,
        [("delete".into(), "target".into())]
    );
    assert_eq!(
        remote.extra_permissions["target"]["permissionDetails"],
        json!([{"inherited":true}])
    );
    assert_eq!(remote.extra_permissions.len(), 4);
}

#[tokio::test]
async fn revocation_reports_owner_and_unknown_permissions_without_deleting_them() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for (permission, reason) in [
        (
            json!({"id":"permission", "type":"user", "emailAddress":"target@example.test", "role":"owner", "permissionDetails":[{"inherited":false}]}),
            RetainedAccessReason::StoreOwner,
        ),
        (
            json!({"id":"permission", "type":"user", "role":"reader", "permissionDetails":[{"inherited":false}]}),
            RetainedAccessReason::UnidentifiedAccount,
        ),
        (
            json!({"id":"permission", "type":"user", "emailAddress":"target@example.test", "role":"reader"}),
            RetainedAccessReason::UnidentifiedAccount,
        ),
    ] {
        state
            .lock()
            .unwrap()
            .extra_permissions
            .insert("permission".into(), permission.clone());
        let MemberRemoval::AccessRemains { shares } = storage
            .revoke_access(&MemberAccess::ProviderAccount("target@example.test".into()))
            .await
            .unwrap()
        else {
            panic!("remaining access was hidden")
        };
        assert_eq!(
            shares,
            [RetainedAccess {
                provider_id: "permission".into(),
                reason
            }]
        );
        assert_eq!(
            state.lock().unwrap().extra_permissions["permission"],
            permission
        );
        assert!(state.lock().unwrap().permission_mutations.is_empty());
    }
}

#[tokio::test]
async fn granting_write_access_adds_a_direct_grant_beside_an_inherited_reader() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state.lock().unwrap().extra_permissions.insert("target".into(), json!({"id":"target", "type":"user", "emailAddress":"target@example.test", "role":"reader", "permissionDetails":[{"inherited":true}]}));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("target@example.test").await.unwrap();
    assert_eq!(
        state.lock().unwrap().extra_permissions["target"]["role"],
        "writer"
    );
    assert_eq!(
        state.lock().unwrap().permission_mutations,
        [("create".into(), "target@example.test".into())]
    );
}

#[tokio::test]
async fn viewer_upgrades_preserve_access_when_refused_and_retry_a_lost_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .permissions
        .insert("target@example.test".into(), "reader".into());
    state.lock().unwrap().refuse_share = true;
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .grant_access("target@example.test")
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(
        state.lock().unwrap().permissions["target@example.test"],
        "reader"
    );
    state.lock().unwrap().refuse_share = false;
    state.lock().unwrap().lose_share_reply = true;
    assert_eq!(
        storage
            .grant_access("target@example.test")
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::Network
    );
    storage.grant_access("target@example.test").await.unwrap();
    assert_eq!(
        state.lock().unwrap().permissions["target@example.test"],
        "writer"
    );
    assert_eq!(
        state.lock().unwrap().permission_mutations,
        [
            ("update".into(), "target@example.test".into()),
            ("update".into(), "target@example.test".into())
        ]
    );
}

#[tokio::test]
async fn conformance_and_account_sharing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    crate::test_utils::Conformance::new(storage.clone())
        .run()
        .await
        .unwrap();
    storage.grant_access("member").await.unwrap();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    assert!(state.lock().unwrap().permissions.is_empty());
}
#[tokio::test]
async fn sharing_requires_the_store_owners_account() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("kept@example.test").await.unwrap();
    state.lock().unwrap().non_owner = true;
    for error in [
        storage
            .grant_access("new@example.test")
            .await
            .err()
            .unwrap(),
        storage
            .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
            .await
            .err()
            .unwrap(),
    ] {
        assert!(matches!(error, StorageError::NotStoreOwner));
        assert_eq!(error.failure(), StorageFailure::PermissionDenied);
    }
    assert_eq!(
        state
            .lock()
            .unwrap()
            .permissions
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["kept@example.test"]
    );
}

#[tokio::test]
async fn deleting_a_path_removes_its_copies_and_preserves_unlinked_bytes() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut remote, &path, "later", "2026-10-06T00:00:01Z", 31);
        duplicate(
            &mut remote,
            &path,
            "other-account",
            "2026-10-06T00:00:02Z",
            32,
        );
        remote.files.get_mut("other-account").unwrap().0["ownedByMe"] = json!(false);
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    state.lock().unwrap().lose_delete_reply = true;
    assert_eq!(
        storage.delete(&path).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.delete(&path).await.unwrap();
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    storage.delete(&path).await.unwrap();
    let remote = state.lock().unwrap();
    assert_eq!(remote.files.len(), 1);
    assert_eq!(remote.files["other-account"].1, b"data");
    assert_eq!(remote.files["other-account"].0["parents"], json!([]));
}
