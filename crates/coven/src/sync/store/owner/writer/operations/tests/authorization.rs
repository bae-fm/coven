use super::*;

#[tokio::test]
async fn merge_operation_authorization_uses_its_exact_predecessor_membership_cut() {
    let owner_db = open_test_db();
    let owner = UserKeypair::generate();
    let writer = UserKeypair::generate();
    let writer_pubkey = pubkey_hex(&writer);
    let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
    let store = TestStore::create(&owner_db, "operation-predecessor-membership", owner.clone())
        .await
        .expect("create Merge Store");
    store
        .invite_member(
            &owner_db,
            &owner,
            &crate::sync::hlc::Hlc::new("operation-predecessor-membership".to_string()),
            &writer_pubkey,
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Operation predecessor membership",
        )
        .await
        .expect("invite operation author");
    let writer_db = open_test_db();
    install_active_device_fixture(
        &store,
        &owner_db,
        &writer_db,
        &writer,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate operation author device");
    promote_active_member_fixture(&store, &owner_db, &writer_db, &owner, &writer, &encryption)
        .await
        .expect("promote operation author to Owner");
    let owner_device = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("bind operation owner");
    let before_removal = owner_device
        .membership_for_test()
        .await
        .expect("load membership at the writer's predecessor");
    let predecessor_authority = before_removal
        .write_grant_authority(&writer_pubkey)
        .expect("predecessor membership authorizes the writer");
    assert!(before_removal.is_owner_now(&writer_pubkey));

    let custody = TestCustody::default();
    let security = crate::sync::test_helpers::test_store_security(
        "operation-membership-removal",
        std::sync::Arc::new(custody),
    );
    store
        .remove_member(
            &owner_db,
            &owner,
            &crate::sync::hlc::Hlc::new("operation-membership-removal".to_string()),
            &writer_pubkey,
            &encryption,
            &security,
        )
        .await
        .expect("remove operation author after its predecessor cut");
    let candidate = owner_device
        .membership_for_test()
        .await
        .expect("load candidate membership after removal");
    assert!(!candidate.can_write_now(&writer_pubkey));

    let plan = prepare_plan(&writer_db, &store.storage, &writer)
        .await
        .expect("authorize operation at its predecessor cut");
    let state = &plan.membership_state;

    assert_eq!(state.heads, before_removal.head_refs());
    assert_eq!(state.resolutions, before_removal.resolution_refs());
    assert_ne!(state.heads, candidate.head_refs());
    assert_eq!(
        plan.membership_authority,
        StoreOperationMembershipAuthority {
            predecessor: predecessor_authority,
        }
    );
    assert_eq!(
        plan.owner_grant,
        before_removal.active_owner_grant(&writer_pubkey)
    );
}

#[tokio::test]
async fn merge_outbound_authorization_rejects_a_direct_cut_older_than_its_predecessor() {
    let owner_db = open_test_db();
    let owner = UserKeypair::generate();
    let writer = UserKeypair::generate();
    let writer_pubkey = pubkey_hex(&writer);
    let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
    let store = TestStore::create(
        &owner_db,
        "direct-removal-predecessor-membership",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    store
        .invite_member(
            &owner_db,
            &owner,
            &crate::sync::hlc::Hlc::new("direct-removal-predecessor-membership".to_string()),
            &writer_pubkey,
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Direct removal predecessor membership",
        )
        .await
        .expect("invite operation author");
    let writer_db = open_test_db();
    install_active_device_fixture(
        &store,
        &owner_db,
        &writer_db,
        &writer,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate operation author device");
    let owner_device = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("bind direct-removal owner");
    let before_removal = owner_device
        .membership_for_test()
        .await
        .expect("load membership before direct removal");
    assert!(before_removal.can_write_now(&writer_pubkey));

    let custody = TestCustody::default();
    let security = crate::sync::test_helpers::test_store_security(
        "direct-membership-removal",
        std::sync::Arc::new(custody),
    );
    store
        .remove_member(
            &owner_db,
            &owner,
            &crate::sync::hlc::Hlc::new("direct-membership-removal".to_string()),
            &writer_pubkey,
            &encryption,
            &security,
        )
        .await
        .expect("remove operation author directly");
    let after_removal = owner_device
        .membership_for_test()
        .await
        .expect("load membership after direct removal");
    assert!(!after_removal.can_write_now(&writer_pubkey));

    host_exec(
        &owner_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('direct-removal-witness', 'witness', NULL, 1, \
                 '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(
        prepare_merge_store_write(&owner_db, &store.storage, &owner, &store_dir,)
            .await
            .expect("prepare removal-witnessing predecessor commit")
    );
    let prepared = crate::database::StoreDatabase::new(&owner_db)
        .oldest_prepared_store_write()
        .await
        .expect("load removal-witnessing predecessor")
        .expect("removal-witnessing predecessor exists");
    let predecessor_membership = &prepared.commit.value.membership_state;
    assert_eq!(predecessor_membership.heads, after_removal.head_refs());
    let predecessor = prepared.head.value.commit.clone();
    assert_eq!(
        drain_store_writes(&owner_db, &store.storage, &owner)
            .await
            .unwrap(),
        1
    );

    let writer_device = store
        .bind_device(&writer_db, &writer)
        .await
        .expect("bind removed writer");
    let crate::protocol::store_commit::StoreCommitCoord { stream_id, .. } = predecessor.coord;
    let order = crate::protocol::store_commit::StoreCommitOrder {
        seq: 1,
        predecessor: None,
        dependencies: std::collections::BTreeMap::from([(stream_id, predecessor)]),
    };
    let result = writer_device
        .authorize_retained_outbound_for_test(&order, before_removal.head_refs())
        .await;

    assert!(
        result.is_err(),
        "an older Direct cut re-authorized a member removed in the exact predecessor"
    );
}

#[tokio::test]
async fn merge_outbound_authorization_admits_direct_membership_after_its_predecessor() {
    let owner_db = open_test_db();
    let owner = UserKeypair::generate();
    let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
    let store = TestStore::create(
        &owner_db,
        "new-direct-predecessor-membership",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let owner_device = store
        .bind_device(&owner_db, &owner)
        .await
        .expect("load predecessor membership");
    host_exec(
        &owner_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('new-direct-witness', 'witness', NULL, 1, \
                 '0000000001000-0000-owner', '2026-01-01')",
    )
    .await;
    let (_temp, store_dir) = temp_store_dir();
    assert!(
        prepare_merge_store_write(&owner_db, &store.storage, &owner, &store_dir,)
            .await
            .expect("prepare predecessor commit")
    );
    let predecessor = crate::database::StoreDatabase::new(&owner_db)
        .oldest_prepared_store_write()
        .await
        .expect("load predecessor commit")
        .expect("predecessor commit exists")
        .head
        .value
        .commit;
    assert_eq!(
        drain_store_writes(&owner_db, &store.storage, &owner)
            .await
            .unwrap(),
        1
    );

    let new_member = UserKeypair::generate();
    let new_member_pubkey = pubkey_hex(&new_member);
    store
        .invite_member(
            &owner_db,
            &owner,
            &crate::sync::hlc::Hlc::new("new-direct-membership".to_string()),
            &new_member_pubkey,
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "New Direct membership",
        )
        .await
        .expect("publish new Direct membership");
    let candidate = owner_device
        .membership_for_test()
        .await
        .expect("load candidate with new Direct membership");
    assert!(candidate.can_write_now(&new_member_pubkey));

    let crate::protocol::store_commit::StoreCommitCoord { stream_id, .. } = predecessor.coord;
    let order = crate::protocol::store_commit::StoreCommitOrder {
        seq: predecessor.coord.sequence() + 1,
        predecessor: Some(predecessor.clone()),
        dependencies: std::collections::BTreeMap::from([(stream_id, predecessor)]),
    };
    let authorization = owner_device
        .authorize_retained_outbound_for_test(&order, candidate.head_refs())
        .await
        .expect("authorize membership that causally extends the predecessor");

    assert_eq!(authorization.membership.head_refs(), candidate.head_refs());
    assert!(authorization.membership.can_write_now(&new_member_pubkey));
}

#[tokio::test]
async fn conflict_resolution_preparation_rejects_a_tampered_local_device_projection() {
    let owner_db = open_test_db();
    let owner = UserKeypair::generate();
    let store = TestStore::create(
        &owner_db,
        "conflict-resolution-remote-device-authority",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let device = store
        .open_into(&owner_db)
        .await
        .expect("load exact founder membership");
    let chain = device
        .membership_for_test()
        .await
        .expect("load exact founder membership");
    let changeset = crate::sync::test_helpers::capture_bytes(
        &open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('remote-device-authority', 'authority', NULL, 1, \
                     '0000000001000-0000-owner', '2026-01-01')",
        ],
    )
    .await;
    store
        .publish_changeset("founder", 1, &changeset, owner_db.schema_version())
        .await
        .expect("publish exact predecessor commit");
    let forged_device_id = crate::protocol::store_commit::StoreDeviceId::derive(
        &store.root,
        &crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: crate::protocol::store_commit::StoreCreationId::from_nonce(
                "forged-local-projection",
            ),
        },
    );
    owner_db
        .call(move |connection| {
            let rows = {
                let mut statement = connection
                    .prepare("SELECT commit_ref, state FROM store_device_state_snapshots")
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                rows
            };
            for (commit, encoded) in rows {
                let state: crate::protocol::store_commit::ResolvedStoreDeviceState =
                    serde_json::from_str(&encoded).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "parse test Store device snapshot: {error}"
                        ))
                    })?;
                let mut forged_registration = state
                    .devices
                    .values()
                    .next()
                    .ok_or_else(|| {
                        crate::database::DbError::Message(
                            "test Store device snapshot has no registration".to_string(),
                        )
                    })?
                    .registration
                    .clone();
                forged_registration.device_id = forged_device_id;
                let forged = state
                    .activate_registration(forged_registration.clone(), None)
                    .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
                connection
                    .execute(
                        "UPDATE store_device_state_snapshots SET state = ?1 \
                         WHERE commit_ref = ?2",
                        rusqlite::params![
                            serde_json::to_string(&forged).map_err(|error| {
                                crate::database::DbError::Message(format!(
                                    "serialize forged Store device snapshot: {error}"
                                ))
                            })?,
                            commit,
                        ],
                    )
                    .map_err(crate::database::DbError::from)?;
            }
            Ok(())
        })
        .await
        .expect("tamper local Store device projection");

    let error = match prepare_merge_conflict_resolution_commit(
        &owner_db,
        &store.storage,
        &owner,
        chain.head_refs(),
    )
    .await
    {
        Ok(_) => panic!("tampered retained Store device state must fail"),
        Err(error) => error,
    };

    assert!(error.to_string().contains(
        "retained Merge checkpoint: Store device state differs from its signed predecessor state"
    ));
}
