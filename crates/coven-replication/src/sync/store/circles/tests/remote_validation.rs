use super::*;

#[tokio::test]
async fn remote_activation_rejects_a_tampered_access_entry_in_a_resigned_control() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let fixture = TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "circle-tampered-access-entry",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Circle test Store");
    let (store, cloud_storage) = fixture;
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind tampered-access Circle object Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    store
        .admit_member(
            &db,
            db_store_dir.clone(),
            &founder,
            &peer_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Tampered access test Store",
        )
        .await
        .expect("admit Store member outside the Circle roster");
    let journal = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare Circle with Store-member access")
        .journal;
    let old_commit = journal.commit().expect("parse prepared Store commit");
    let author = coven_database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    // The Owner seals the peer's slot with a leaf naming a foreign Store
    // membership. Preparation re-signs and re-seals it, so the control's entry
    // is the exact sealed bytes of this leaf — the only surviving contradiction
    // is between the leaf's signed context and the control it rides in.
    let mut draft = draft_from_transition(&journal.operation().creation);
    let peer_leaf = draft
        .access
        .iter_mut()
        .find(|access| access.value.recipient_pubkey == peer_pubkey)
        .expect("Store member has a prepared access leaf");
    peer_leaf.value.body_mut().store_membership =
        draft.control.value.access_epoch().store_membership.clone();
    peer_leaf.value.body_mut().store_membership.state_hash =
        ObjectHash::digest(b"foreign Store membership state");
    let (creation, objects, prepared, control_head_object, stream_activations) = device
        .prepare_circle_activation_objects(draft, &journal.operation().history)
        .await
        .expect("prepare exact tampered access objects");
    for object in prepared.values() {
        cloud_storage
            .create_protocol_object(object)
            .await
            .expect("publish exact tampered access object");
    }
    let commit_coord = journal.operation().commit_ref().coord.clone();
    let circle_reference = creation.control_ref(objects, control_head_object);
    let commit = device
        .sign_circle_commit(
            &old_commit,
            commit_coord.clone(),
            circle_reference,
            stream_activations,
        )
        .await
        .expect("sign tampered access commit");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = device
        .prepare_circle_object(
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare tampered access Store commit");
    cloud_storage
        .create_protocol_object(&commit_prepared)
        .await
        .expect("publish tampered access Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind tampered access Store commit");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &peer)
        .await
        .expect("bind tampered-access Circle Store")
        .load_circle_activations(&commit_ref, &commit, author.value())
        .await
        .expect_err("a recipient must reject an access entry that contradicts its control");
    assert!(
        error
            .to_string()
            .contains("circle access leaf failed context verification"),
        "{error}"
    );
}

#[tokio::test]
async fn remote_activation_rejects_active_access_for_a_nonmember() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let fixture = TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "circle-active-access-nonmember",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Circle test Store");
    let (store, cloud_storage) = fixture;
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind promoted-access Circle object Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    store
        .admit_member(
            &db,
            db_store_dir.clone(),
            &founder,
            &peer_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Active access test Store",
        )
        .await
        .expect("admit Store member outside the Circle roster");
    let journal = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare Circle with inactive Store-member access")
        .journal;
    let old_commit = journal.commit().expect("parse prepared Store commit");
    let author = coven_database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    let mut draft = draft_from_transition(&journal.operation().creation);
    promote_store_member_access_without_adding_to_circle_roster(&mut draft, &founder, &peer);
    let (creation, objects, prepared, control_head_object, stream_activations) = device
        .prepare_circle_activation_objects(draft, &journal.operation().history)
        .await
        .expect("prepare exact promoted access objects");
    for object in prepared.values() {
        cloud_storage
            .create_protocol_object(object)
            .await
            .expect("publish exact promoted access object");
    }
    let commit_coord = journal.operation().commit_ref().coord.clone();
    let circle_reference = creation.control_ref(objects, control_head_object);
    let commit = device
        .sign_circle_commit(
            &old_commit,
            commit_coord.clone(),
            circle_reference,
            stream_activations,
        )
        .await
        .expect("sign promoted access commit");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = device
        .prepare_circle_object(
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare promoted access Store commit");
    cloud_storage
        .create_protocol_object(&commit_prepared)
        .await
        .expect("publish promoted access Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind promoted access Store commit");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &peer)
        .await
        .expect("bind promoted-access Circle Store")
        .load_circle_activations(&commit_ref, &commit, author.value())
        .await
        .expect_err("Active access must name a resolved Circle member");
    assert!(
        error
            .to_string()
            .contains("Active access recipient is absent"),
        "{error}"
    );
}

#[tokio::test]
async fn candidate_graph_rejects_an_access_leaf_that_differs_from_its_control() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &db,
        db_store_dir.clone(),
        "circle-substituted-access-leaf",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create exact Circle test Store");
    let mut prepared = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Substituted access leaf")
        .await
        .expect("prepare Circle operation");
    let leaf = &mut prepared.journal.operation_mut().creation.access[0];
    leaf.value.body_mut().disposition = CircleAccessDisposition::Inactive;
    leaf.value.resign(&founder);

    let error = prepared
        .journal
        .closed_remote_objects(&prepared.prepared_objects)
        .expect_err("a leaf that differs from its control entry must not acquire ownership");
    assert!(
        error
            .to_string()
            .contains("differs from its signed control entry"),
        "{error}"
    );
}

#[tokio::test]
async fn inactive_circle_member_verifies_public_first_head_activations() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let founder = UserKeypair::generate();
    let store = TestStore::create(
        &db,
        db_store_dir.clone(),
        "circle-inactive-member",
        founder.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Circle Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    store
        .admit_member(
            &db,
            db_store_dir.clone(),
            &founder,
            &peer_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([43; 32]),
            "Inactive Circle member Store",
        )
        .await
        .expect("admit Store member outside the Circle");
    let prepared = store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare founder Circle");
    let commit = prepared
        .journal
        .commit()
        .expect("parse founder Circle commit");
    let commit_ref = prepared.journal.operation().commit_ref().clone();
    coven_database::StoreDatabase::new(&db)
        .insert_circle_operation(prepared.journal, prepared.prepared_objects)
        .await
        .expect("persist founder Circle operation");
    store
        .bind_device_in(&db, db_store_dir.clone(), &founder)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("publish founder Circle");
    let author = coven_database::StoreDatabase::new(&db)
        .activated_store_device_registration(commit.author_registration.clone())
        .await
        .expect("load founder device registration");
    let verified = store
        .bind_device_in(&db, db_store_dir.clone(), &peer)
        .await
        .expect("bind inactive-member Circle Store")
        .load_circle_activations(&commit_ref, &commit, author.value())
        .await
        .expect("inactive Circle member verifies the public activation graph");
    let [circle] = verified.circles() else {
        panic!("founder commit must activate one Circle")
    };
    assert!(matches!(
        circle
            .local_access
            .as_ref()
            .expect("Store member receives an inactive leaf")
            .leaf
            .value
            .disposition,
        CircleAccessDisposition::Inactive
    ));
}

#[tokio::test]
async fn remote_activation_rejects_metadata_with_a_different_historical_roster() {
    let baseline_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let baseline_db = crate::sync::test_helpers::open_test_db(baseline_db_store_dir.clone());
    let (baseline_store, _home, baseline_signer, baseline) = persist_merge_operation(
        &baseline_db,
        baseline_db_store_dir.clone(),
        "circle-remote-metadata-baseline",
    )
    .await;
    let baseline_commit = baseline.commit().expect("parse baseline Store commit");
    publish_prepared_objects(&baseline_store, &baseline_db, &baseline).await;
    let baseline_author = StoreDatabase::new(&baseline_db)
        .activated_store_device_registration(baseline_commit.author_registration.clone())
        .await
        .expect("load baseline exact Circle commit author");
    baseline_store
        .bind_device_in(
            &baseline_db,
            baseline_db_store_dir.clone(),
            &baseline_signer,
        )
        .await
        .expect("bind baseline Circle activation Store")
        .load_circle_activations(
            baseline.operation().commit_ref(),
            &baseline_commit,
            baseline_author.value(),
        )
        .await
        .expect("baseline exact Circle activation verifies remotely");

    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (fixture, _home, signer, founder_journal) =
        persist_merge_operation_fixture(&db, db_store_dir.clone(), "circle-remote-metadata-roster")
            .await;
    let (store, cloud_storage) = fixture;
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind mismatched-roster Circle object Store");
    let circle_id = founder_journal.circle_id();
    store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("publish exact founder Circle");
    _home.fail_exact_create_before_call(1);
    device
        .rename_circle("0000000002000-0000-creator", circle_id, "Renamed household")
        .await
        .expect_err("interrupt rename before its first exact upload");
    let operation_id = coven_database::StoreDatabase::new(&db)
        .get_circle_operations()
        .await
        .expect("list interrupted Circle rename")
        .into_iter()
        .find(|operation| operation.circle_id == circle_id)
        .expect("interrupted Circle rename remains pending")
        .operation_id;
    let journal = coven_database::StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read interrupted Circle rename")
        .expect("interrupted Circle rename journal remains durable");
    let old_commit = journal.commit().expect("parse prepared Store commit");
    let commit_coord = journal.operation().commit_ref().coord.clone();
    let mut draft = draft_from_transition(&journal.operation().creation);
    let store_root_hash = draft.control.value.store_root_hash;
    let roster = &mut draft.roster;
    roster.state_hash = ObjectHash::digest(b"different historical roster state");
    let author = coven_database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    let (creation, objects, prepared, control_head_object, stream_activations) = device
        .prepare_circle_activation_objects(draft, &journal.operation().history)
        .await
        .expect("prepare forged exact Circle activation objects");
    for object in prepared.values() {
        cloud_storage
            .create_protocol_object(object)
            .await
            .expect("publish forged exact Circle activation object");
    }
    let circle_reference = creation.control_ref(objects, control_head_object);
    let commit = device
        .sign_circle_commit(
            &old_commit,
            commit_coord.clone(),
            circle_reference,
            stream_activations,
        )
        .await
        .expect("sign forged metadata activation commit");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = device
        .prepare_circle_object(
            &ProtocolObjectContext::signed_plaintext(
                store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            &commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id.to_string(),
                commit.seq(),
                commit.commit_hash(),
            ),
            ".json",
            commit.to_bytes(),
        )
        .await
        .expect("prepare forged exact Store commit");
    cloud_storage
        .create_protocol_object(&commit_prepared)
        .await
        .expect("publish forged exact Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind forged exact Store commit reference");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind mismatched-roster Circle Store")
        .load_circle_activations(&commit_ref, &commit, author.value())
        .await
        .expect_err("metadata cannot borrow authority from a different roster state");
    assert!(
        error.to_string().contains("roster state hash differs"),
        "{error}"
    );
}
