use super::*;

#[tokio::test]
async fn remote_activation_rejects_invented_access_refs_in_a_resigned_commit() {
    let db = open_test_db();
    let (store, signer, journal) =
        persist_merge_operation(&db, "circle-invented-access-refs").await;
    let old_commit = journal.commit().expect("parse prepared Store commit");
    for object in journal.operation().prepared_objects.values() {
        crate::sync::store_objects::create_exact_object(&store.storage, object)
            .await
            .expect("publish original exact Circle activation object");
    }
    let mut objects = old_commit
        .operations()
        .expect("Circle commit carries operations")
        .circle_controls[0]
        .objects()
        .clone();
    let original_ref = objects.access[0].clone();
    let original_access = &journal.operation().creation.access[0];
    let invented_recipient_slot = format!("{}-invented", original_ref.leaf.recipient_slot);
    let candidate_family = old_commit.candidate_family();
    let leaf_prefix = circle_access_leaf_semantic_prefix(
        journal.operation().creation.circle_id,
        candidate_family,
        &original_ref.leaf.owner_pubkey,
        original_ref.leaf.epoch_id,
        &invented_recipient_slot,
        original_ref.leaf.leaf_id,
    );
    let leaf = prepare_circle_object(
        &store.storage,
        &ProtocolObjectContext::recipient_sealed(
            old_commit.store_root_hash,
            ProtocolObjectDomain::CircleAccessLeaf,
        ),
        &leaf_prefix,
        "",
        original_access.leaf.bytes.clone(),
    )
    .await
    .expect("prepare invented access leaf path");
    let envelope_prefix = circle_access_envelope_semantic_prefix(
        journal.operation().creation.circle_id,
        candidate_family,
        &original_ref.envelope.owner_pubkey,
        &invented_recipient_slot,
        original_ref.envelope.control_hash,
    );
    let envelope = prepare_circle_object(
        &store.storage,
        &ProtocolObjectContext::store_encrypted(
            old_commit.store_root_hash,
            ProtocolObjectDomain::CircleAccessEnvelope,
        ),
        &envelope_prefix,
        ".json",
        serde_json::to_vec(&original_access.envelope).expect("serialize original access envelope"),
    )
    .await
    .expect("prepare invented access envelope path");
    crate::sync::store_objects::create_exact_object(&store.storage, &leaf)
        .await
        .expect("publish invented access leaf path");
    crate::sync::store_objects::create_exact_object(&store.storage, &envelope)
        .await
        .expect("publish invented access envelope path");
    objects.access.push(CircleAccessObjectRef {
        leaf: CircleAccessLeafObjectRef {
            recipient_slot: invented_recipient_slot.clone(),
            object: leaf.reference().clone(),
            ..original_ref.leaf
        },
        envelope: CircleAccessEnvelopeObjectRef {
            recipient_slot: invented_recipient_slot,
            object: envelope.reference().clone(),
            ..original_ref.envelope
        },
        bootstrap: None,
    });
    let author = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    let device_signer = author
        .device_signer(&signer)
        .expect("derive Circle commit device signer");
    let commit_coord = journal.operation().commit_ref.coord.clone();
    let original_control = &old_commit.circle_controls()[0];
    let circle_reference = journal
        .operation()
        .creation
        .control_ref(objects, Some(original_control.head_object().clone()));
    let stream_activations = old_commit.stream_activations().to_vec();
    let commit = signed_circle_commit(
        old_commit.store_root_hash,
        old_commit.write_id.clone(),
        commit_coord.clone(),
        old_commit.author_registration.clone(),
        &author,
        old_commit.order.clone(),
        old_commit.membership_state.clone(),
        old_commit.device_state.clone(),
        old_commit
            .operations_membership_authority()
            .expect("prepared Circle commit carries validated operations"),
        circle_reference,
        stream_activations,
        &device_signer,
    )
    .expect("sign commit naming invented access refs");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = prepare_circle_object(
        &store.storage,
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
    .expect("prepare re-signed Store commit");
    crate::sync::store_objects::create_exact_object(&store.storage, &commit_prepared)
        .await
        .expect("publish re-signed Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind re-signed Store commit reference");

    let error = load_circle_activations(
        &db,
        &store.storage,
        &store.root,
        &commit_ref,
        &commit,
        &author,
        &signer,
        &keys::public_key_hex(&signer),
    )
    .await
    .expect_err("invented access references must fail activation");
    assert!(
        error
            .to_string()
            .contains("circle access envelope failed verification"),
        "{error}"
    );

    let original_head = &journal.operation().policy.head;
    let forged_head = StoreDeviceHead::signed(
        commit.store_root_hash,
        commit.author_registration.clone(),
        commit_ref.clone(),
        original_head.history_summary,
        original_head.successor.clone(),
        &device_signer,
    )
    .expect("sign Store head naming the re-signed commit");
    let original_head_object = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Circle operation carries its Store head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        commit.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let forged_head_object = store
        .storage
        .prepare_protocol_object(
            &head_context,
            original_head_object.reference().slot().clone(),
            &head_slot_prefix(
                &commit.author_registration.device_id.to_string(),
                commit.seq(),
            ),
            forged_head.to_bytes(),
        )
        .expect("prepare Store head naming the re-signed commit");
    store.home.replace_exact_object(
        original_head_object.reference().slot(),
        forged_head_object.stored_bytes().to_vec(),
    );

    let (_store_temp, store_dir) = temp_store_dir();
    let authorized_store = crate::sync::store::Store::authorize_borrowed(&store.storage, &db)
        .await
        .expect("authorize Store pull");
    let pull = authorized_store
        .pull(&store_dir, &signer, None)
        .await
        .expect("pull reports the invented access commit as held");
    assert!(pull.held_positions.iter().any(|held| {
        matches!(
            &held.reason,
            crate::sync::store::pull::HeldStorePositionReason::InvalidObject(reason)
                if reason.contains("circle access envelope failed verification")
        )
    }));
    assert_eq!(activation_count(&db, journal.circle_id()).await, 0);
    assert!(crate::sync::store::database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id.to_string(), commit.seq())
        .await
        .expect("read invented access commit position")
        .is_none());
}

#[tokio::test]
async fn remote_activation_rejects_active_access_for_a_nonmember() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&db, "circle-active-access-nonmember", founder.clone())
        .await
        .expect("create exact Circle test Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("founder-device".to_string()),
        &peer_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([42; 32]),
        "circle-active-access-nonmember",
        "Active access test Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member outside the Circle roster");
    let device_id = local_device_id(&db).await;
    let journal = prepare_circle_operation(
        &db,
        &store.storage,
        &device_id,
        "0000000001000-0000-founder",
        "Household",
        &founder,
    )
    .await
    .expect("prepare Circle with inactive Store-member access");
    let old_commit = journal.commit().expect("parse prepared Store commit");
    let candidate_family = old_commit.candidate_family();
    let author = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    let device_signer = author
        .device_signer(&founder)
        .expect("derive Circle commit device signer");
    let mut draft = draft_from_transition(&journal.operation().creation);
    promote_store_member_access_without_adding_to_circle_roster(&mut draft, &founder, &peer);
    let (creation, objects, prepared, control_head_object, stream_activations) =
        prepare_circle_activation_objects(
            &store.storage,
            &store.root,
            draft,
            &journal.operation().history,
            candidate_family,
            &old_commit.author_registration,
            &author,
            &founder,
            &device_signer,
        )
        .await
        .expect("prepare exact promoted access objects");
    for object in prepared.values() {
        crate::sync::store_objects::create_exact_object(&store.storage, object)
            .await
            .expect("publish exact promoted access object");
    }
    let commit_coord = journal.operation().commit_ref.coord.clone();
    let circle_reference = creation.control_ref(objects, control_head_object);
    let commit = signed_circle_commit(
        old_commit.store_root_hash,
        old_commit.write_id.clone(),
        commit_coord.clone(),
        old_commit.author_registration.clone(),
        &author,
        old_commit.order.clone(),
        old_commit.membership_state.clone(),
        old_commit.device_state.clone(),
        old_commit
            .operations_membership_authority()
            .expect("prepared Circle commit carries validated operations"),
        circle_reference,
        stream_activations,
        &device_signer,
    )
    .expect("sign promoted access commit");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = prepare_circle_object(
        &store.storage,
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
    crate::sync::store_objects::create_exact_object(&store.storage, &commit_prepared)
        .await
        .expect("publish promoted access Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind promoted access Store commit");

    let error = load_circle_activations(
        &db,
        &store.storage,
        &store.root,
        &commit_ref,
        &commit,
        &author,
        &peer,
        &keys::public_key_hex(&founder),
    )
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
async fn candidate_graph_rejects_partial_circle_access_ownership() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&db, "circle-partial-access-graph", founder.clone())
        .await
        .expect("create exact Circle test Store");
    let device_id = local_device_id(&db).await;
    let mut journal = prepare_circle_operation(
        &db,
        &store.storage,
        &device_id,
        "0000000001000-0000-founder",
        "Partial access graph",
        &founder,
    )
    .await
    .expect("prepare Circle operation");
    let commit = journal.commit().expect("parse Circle commit");
    let envelope_object = commit.circle_controls()[0].objects().access[0]
        .envelope
        .object
        .clone();
    let removed = journal
        .operation_mut()
        .prepared_objects
        .iter()
        .find(|(_, prepared)| prepared.reference() == &envelope_object)
        .map(|(step, _)| step.clone())
        .expect("find prepared access envelope");
    journal.operation_mut().prepared_objects.remove(&removed);

    let error = journal
        .closed_remote_objects()
        .expect_err("a leaf without its envelope must not acquire candidate ownership");
    assert!(
        error.to_string().contains("has no prepared bytes"),
        "{error}"
    );
}

#[tokio::test]
async fn inactive_circle_member_verifies_public_first_head_activations() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = TestStore::create(&db, "circle-inactive-member", founder.clone())
        .await
        .expect("create Circle Store");
    let peer = UserKeypair::generate();
    let peer_pubkey = keys::public_key_hex(&peer);
    crate::sync::store::membership::invite_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &crate::sync::hlc::Hlc::new("founder-device".to_string()),
        &peer_pubkey,
        None,
        MemberRole::Member,
        &EncryptionService::from_key([43; 32]),
        "circle-inactive-member",
        "Inactive Circle member Store",
        &StoreDatabase::new(&db),
    )
    .await
    .expect("invite Store member outside the Circle");
    let device_id = local_device_id(&db).await;
    let journal = prepare_circle_operation(
        &db,
        &store.storage,
        &device_id,
        "0000000001000-0000-founder",
        "Household",
        &founder,
    )
    .await
    .expect("prepare founder Circle");
    let commit = journal.commit().expect("parse founder Circle commit");
    let commit_ref = journal.operation().commit_ref.clone();
    StoreDatabase::new(&db)
        .insert_circle_operation(journal.clone())
        .await
        .expect("persist founder Circle operation");
    resume_circle_operations(&db, &store.storage, &founder)
        .await
        .expect("publish founder Circle");
    let author = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration(commit.author_registration.clone())
        .await
        .expect("load founder device registration");
    let verified = load_circle_activations(
        &db,
        &store.storage,
        &store.root,
        &commit_ref,
        &commit,
        &author,
        &peer,
        &keys::public_key_hex(&founder),
    )
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
    let baseline_db = open_test_db();
    let (baseline_store, baseline_signer, baseline) =
        persist_merge_operation(&baseline_db, "circle-remote-metadata-baseline").await;
    let baseline_commit = baseline.commit().expect("parse baseline Store commit");
    for object in baseline.operation().prepared_objects.values() {
        crate::sync::store_objects::create_exact_object(&baseline_store.storage, object)
            .await
            .expect("publish baseline exact Circle activation object");
    }
    let baseline_author = StoreDatabase::new(&baseline_db)
        .activated_store_device_registration(baseline_commit.author_registration.clone())
        .await
        .expect("load baseline exact Circle commit author");
    load_circle_activations(
        &baseline_db,
        &baseline_store.storage,
        &baseline_store.root,
        &baseline.operation().commit_ref,
        &baseline_commit,
        &baseline_author,
        &baseline_signer,
        &keys::public_key_hex(&baseline_signer),
    )
    .await
    .expect("baseline exact Circle activation verifies remotely");

    let db = open_test_db();
    let (store, signer, founder_journal) =
        persist_merge_operation(&db, "circle-remote-metadata-roster").await;
    let circle_id = founder_journal.circle_id();
    resume_circle_operations(&db, &store.storage, &signer)
        .await
        .expect("publish exact founder Circle");
    store.home.fail_exact_create_before_call(1);
    rename_circle(
        &db,
        &store.storage,
        &local_device_id(&db).await,
        "0000000002000-0000-creator",
        circle_id,
        "Renamed household",
        &signer,
    )
    .await
    .expect_err("interrupt rename before its first exact upload");
    let operation_id = StoreDatabase::new(&db)
        .get_circle_operations()
        .await
        .expect("list interrupted Circle rename")
        .into_iter()
        .find(|operation| operation.circle_id == circle_id)
        .expect("interrupted Circle rename remains pending")
        .operation_id;
    let journal = StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read interrupted Circle rename")
        .expect("interrupted Circle rename journal remains durable");
    let old_commit = journal.commit().expect("parse prepared Store commit");
    let commit_coord = journal.operation().commit_ref.coord.clone();
    let mut draft = draft_from_transition(&journal.operation().creation);
    let store_root_hash = draft.control.value.store_root_hash;
    let roster = &mut draft.roster;
    roster.state_hash = ObjectHash::digest(b"different historical roster state");
    let candidate_family = old_commit.candidate_family();
    let author = crate::sync::store::database::StoreDatabase::new(&db)
        .activated_store_device_registration(old_commit.author_registration.clone())
        .await
        .expect("load exact Circle commit author");
    let device_signer = author
        .device_signer(&signer)
        .expect("derive Circle commit device signer");
    let (creation, objects, prepared, control_head_object, stream_activations) =
        prepare_circle_activation_objects(
            &store.storage,
            &store.root,
            draft,
            &journal.operation().history,
            candidate_family,
            &old_commit.author_registration,
            &author,
            &signer,
            &device_signer,
        )
        .await
        .expect("prepare forged exact Circle activation objects");
    for object in prepared.values() {
        crate::sync::store_objects::create_exact_object(&store.storage, object)
            .await
            .expect("publish forged exact Circle activation object");
    }
    let circle_reference = creation.control_ref(objects, control_head_object);
    let commit = signed_circle_commit(
        store_root_hash,
        old_commit.write_id.clone(),
        commit_coord.clone(),
        old_commit.author_registration.clone(),
        &author,
        old_commit.order.clone(),
        old_commit.membership_state.clone(),
        old_commit.device_state.clone(),
        old_commit
            .operations_membership_authority()
            .expect("prepared Circle commit carries validated operations"),
        circle_reference,
        stream_activations,
        &device_signer,
    )
    .expect("sign forged metadata activation commit");
    let StoreCommitCoord { stream_id, .. } = commit_coord.clone();
    let commit_prepared = prepare_circle_object(
        &store.storage,
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
    crate::sync::store_objects::create_exact_object(&store.storage, &commit_prepared)
        .await
        .expect("publish forged exact Store commit");
    let commit_ref = StoreBatchCommitRef::from_commit(
        &commit,
        commit_coord,
        commit_prepared.reference().clone(),
    )
    .expect("bind forged exact Store commit reference");

    let error = load_circle_activations(
        &db,
        &store.storage,
        &store.root,
        &commit_ref,
        &commit,
        &author,
        &signer,
        &keys::public_key_hex(&signer),
    )
    .await
    .expect_err("metadata cannot borrow authority from a different roster state");
    assert!(
        error.to_string().contains("roster state hash differs"),
        "{error}"
    );
}
