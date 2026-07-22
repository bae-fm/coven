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
    super::super::super::membership_ops::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &super::super::super::hlc::Hlc::new("operation-predecessor-membership".to_string()),
        &writer_pubkey,
        None,
        super::super::super::membership::MemberRole::Member,
        &encryption,
        store.storage.store_id(),
        "Operation predecessor membership",
        &owner_db,
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
    let before_removal = super::super::super::membership_ops::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&pubkey_hex(&owner)),
        Some(&owner_db),
    )
    .await
    .expect("load membership at the writer's predecessor");
    let predecessor_authority = before_removal
        .write_grant_authority(&writer_pubkey)
        .expect("predecessor membership authorizes the writer");
    assert!(before_removal.is_owner_now(&writer_pubkey));

    let custody = TestCustody::default();
    let cipher = RwLock::new(CloudCipher::Encrypted(encryption.clone()));
    super::super::super::membership_ops::remove_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &super::super::super::hlc::Hlc::new("operation-membership-removal".to_string()),
        &writer_pubkey,
        &encryption,
        &custody,
        &cipher,
        &PendingRotation::none(),
        &owner_db,
    )
    .await
    .expect("remove operation author after its predecessor cut");
    let candidate = super::super::super::membership_ops::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&pubkey_hex(&owner)),
        Some(&owner_db),
    )
    .await
    .expect("load candidate membership after removal");
    assert!(!candidate.can_write_now(&writer_pubkey));

    let plan = prepare_store_operation_commit(
        &writer_db,
        &store.storage,
        StoreOperationPreparation::MergeConcurrent {
            membership: &candidate,
        },
        &local_device_id(&writer_db).await,
        &writer,
    )
    .await
    .expect("authorize operation at its predecessor cut");
    let super::super::super::circle_control::StoreMembershipStateRef::MergeConcurrent(state) =
        &plan.membership_state
    else {
        panic!("Merge operation produced Serial membership state")
    };

    assert_eq!(state.heads, before_removal.head_refs());
    assert_eq!(state.resolutions, before_removal.resolution_refs());
    assert_ne!(state.heads, candidate.head_refs());
    assert_eq!(
        plan.membership_authority,
        StoreOperationMembershipAuthority::MergeConcurrent {
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
    super::super::super::membership_ops::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &super::super::super::hlc::Hlc::new("direct-removal-predecessor-membership".to_string()),
        &writer_pubkey,
        None,
        super::super::super::membership::MemberRole::Member,
        &encryption,
        store.storage.store_id(),
        "Direct removal predecessor membership",
        &owner_db,
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
    let before_removal = super::super::super::membership_ops::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&pubkey_hex(&owner)),
        Some(&owner_db),
    )
    .await
    .expect("load membership before direct removal");
    assert!(before_removal.can_write_now(&writer_pubkey));

    let custody = TestCustody::default();
    let cipher = RwLock::new(CloudCipher::Encrypted(encryption.clone()));
    super::super::super::membership_ops::remove_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &super::super::super::hlc::Hlc::new("direct-membership-removal".to_string()),
        &writer_pubkey,
        &encryption,
        &custody,
        &cipher,
        &PendingRotation::none(),
        &owner_db,
    )
    .await
    .expect("remove operation author directly");
    let after_removal = super::super::super::membership_ops::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&pubkey_hex(&owner)),
        Some(&owner_db),
    )
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
    assert!(prepare_merge_store_write(
        &owner_db,
        &store.storage,
        &local_device_id(&owner_db).await,
        "2026-07-21T00:01:00Z",
        &owner,
        &store_dir,
        &after_removal,
    )
    .await
    .expect("prepare removal-witnessing predecessor commit"));
    let prepared = owner_db
        .oldest_prepared_store_write()
        .await
        .expect("load removal-witnessing predecessor")
        .expect("removal-witnessing predecessor exists");
    let super::super::super::circle_control::StoreMembershipStateRef::MergeConcurrent(
        predecessor_membership,
    ) = &prepared.commit.value.membership_state
    else {
        panic!("Merge predecessor produced Serial membership state")
    };
    assert_eq!(predecessor_membership.heads, after_removal.head_refs());
    let predecessor = prepared.head.value.commit.clone();
    assert_eq!(
        drain_store_writes(&owner_db, &store.storage).await.unwrap(),
        1
    );

    let (root, registration_ref, _, _) =
        load_local_store_authority(&writer_db, &local_device_id(&writer_db).await, &writer)
            .await
            .expect("load removed writer registration");
    let super::super::super::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } =
        predecessor.coord
    else {
        panic!("Merge predecessor has Serial coordinate")
    };
    let order = super::super::super::store_commit::StoreCommitOrder::MergeConcurrent {
        seq: 1,
        predecessor: None,
        dependencies: std::collections::BTreeMap::from([(stream_id, predecessor)]),
    };
    let result = super::super::super::store_pull::load_retained_merge_outbound_authorization(
        &writer_db,
        &store.storage,
        &root,
        &order,
        before_removal.head_refs(),
        &registration_ref,
    )
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
    let owner_pubkey = pubkey_hex(&owner);
    let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
    let store = TestStore::create(
        &owner_db,
        "new-direct-predecessor-membership",
        owner.clone(),
    )
    .await
    .expect("create Merge Store");
    let predecessor_membership =
        super::super::super::membership_ops::load_and_persist_owner_anchor(
            &store.storage,
            &store.root,
            &owner_pubkey,
            &owner_db,
        )
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
    assert!(prepare_merge_store_write(
        &owner_db,
        &store.storage,
        &local_device_id(&owner_db).await,
        "2026-07-21T00:00:00Z",
        &owner,
        &store_dir,
        &predecessor_membership,
    )
    .await
    .expect("prepare predecessor commit"));
    let predecessor = owner_db
        .oldest_prepared_store_write()
        .await
        .expect("load predecessor commit")
        .expect("predecessor commit exists")
        .head
        .value
        .commit;
    assert_eq!(
        drain_store_writes(&owner_db, &store.storage).await.unwrap(),
        1
    );

    let new_member = UserKeypair::generate();
    let new_member_pubkey = pubkey_hex(&new_member);
    super::super::super::membership_ops::invite_member(
        &store.storage,
        store.home.as_ref(),
        &owner,
        &super::super::super::hlc::Hlc::new("new-direct-membership".to_string()),
        &new_member_pubkey,
        None,
        super::super::super::membership::MemberRole::Member,
        &encryption,
        store.storage.store_id(),
        "New Direct membership",
        &owner_db,
    )
    .await
    .expect("publish new Direct membership");
    let candidate = super::super::super::membership_ops::load_current_exact_chain(
        &store.storage,
        &store.root,
        Some(&owner_pubkey),
        Some(&owner_db),
    )
    .await
    .expect("load candidate with new Direct membership");
    assert!(candidate.can_write_now(&new_member_pubkey));

    let (root, registration_ref, _, _) =
        load_local_store_authority(&owner_db, &local_device_id(&owner_db).await, &owner)
            .await
            .expect("load owner registration");
    let super::super::super::store_commit::StoreCommitCoord::MergeConcurrent { stream_id, .. } =
        predecessor.coord
    else {
        panic!("Merge predecessor has Serial coordinate")
    };
    let order = super::super::super::store_commit::StoreCommitOrder::MergeConcurrent {
        seq: predecessor.coord.sequence() + 1,
        predecessor: Some(predecessor.clone()),
        dependencies: std::collections::BTreeMap::from([(stream_id, predecessor)]),
    };
    let authorization =
        super::super::super::store_pull::load_retained_merge_outbound_authorization(
            &owner_db,
            &store.storage,
            &root,
            &order,
            candidate.head_refs(),
            &registration_ref,
        )
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
    let chain = store
        .open_into(&owner_db)
        .await
        .expect("load exact founder membership");
    let changeset = super::super::super::test_helpers::capture_bytes(
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
    let device_id = local_device_id(&owner_db).await;
    let (_, mut forged_registration, _, _) =
        load_local_store_authority(&owner_db, &device_id, &owner)
            .await
            .expect("load exact local authority");
    forged_registration.device_id = super::super::super::store_commit::StoreDeviceId::derive(
        &store.root,
        &super::super::super::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: super::super::super::store_commit::StoreCreationId::from_nonce(
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
                let state: super::super::super::store_commit::ResolvedStoreDeviceState =
                    serde_json::from_str(&encoded).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "parse test Store device snapshot: {error}"
                        ))
                    })?;
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
        &device_id,
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
