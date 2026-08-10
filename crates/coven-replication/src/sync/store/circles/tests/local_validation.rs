use super::*;
#[tokio::test]
async fn local_activation_rejects_sealed_leaf_plaintext_substitution() {
    let db = open_test_db();
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, "circle-mismatched-local-keyring").await;
    let author = keys::public_key_hex(&signer);
    let own_access = journal
        .operation_mut()
        .creation
        .access
        .iter_mut()
        .find(|access| access.leaf.value.recipient_pubkey == author)
        .expect("founder access");
    let CircleAccessDisposition::Active { keyring, .. } =
        &mut own_access.leaf.value.body_mut().disposition
    else {
        panic!("founder access must be active")
    };
    *keyring = MasterKeyring::generate().to_serialized();
    own_access.leaf.value.resign(&signer);
    own_access.envelope.body_mut().value_hash = ObjectHash::digest(
        &serde_json::to_vec(&own_access.leaf.value).expect("serialize mismatched access leaf"),
    );
    own_access.envelope.resign(&signer);
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted journal plaintext");
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local activation must reject substituted journal plaintext");
    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

#[tokio::test]
async fn local_publication_rejects_a_prepared_object_outside_the_signed_graph() {
    let db = open_test_db();
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, "circle-substituted-local-object-ref").await;
    let original = journal
        .operation()
        .prepared_objects
        .get("metadata")
        .expect("operation carries exact metadata object");
    let substituted_slot = coven_protocol::objects::ObjectSlot::opaque(
        original.slot().logical_key().to_string(),
        "substituted-metadata-object".to_string(),
    )
    .expect("construct alternate provider object slot");
    // Only the slot moves: the same bytes are already spooled under the same
    // hash, so publication finds them and rejects the object on its slot alone.
    let substituted = coven_protocol::objects::ExactObjectRef::new(
        substituted_slot,
        original.stored_size(),
        original.stored_hash(),
    );
    journal
        .operation_mut()
        .prepared_objects
        .insert("metadata".to_string(), substituted);
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted journal object");

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local publication must reject objects outside the signed graph");

    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

#[tokio::test]
async fn local_activation_rejects_substituted_exact_circle_edges() {
    let db = open_test_db();
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, "circle-substituted-signed-edges").await;
    let old_commit = journal.commit().expect("parse prepared Circle commit");
    let [old_reference] = old_commit.circle_controls() else {
        panic!("Circle operation must carry one control")
    };
    let mut objects = old_reference.objects().clone();
    {
        let roster_coord = objects
            .roster_entries
            .keys()
            .next()
            .cloned()
            .expect("founder graph carries a roster entry");
        let metadata_coord = objects
            .metadata_entries
            .keys()
            .next()
            .cloned()
            .expect("founder graph carries metadata");
        let roster = objects
            .roster_entries
            .remove(&roster_coord)
            .expect("remove exact roster edge");
        let metadata = objects
            .metadata_entries
            .get_mut(&metadata_coord)
            .expect("load exact metadata edge");
        let metadata_object = std::mem::replace(&mut metadata.object, roster);
        objects.roster_entries.insert(roster_coord, metadata_object);
    }
    let reference = journal
        .operation()
        .creation
        .control_ref(objects, Some(old_reference.head_object().clone()));
    let device = store
        .bind_device(&db, &signer)
        .await
        .expect("bind substituted Circle object Store");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize substituted Circle object Store");
    writer
        .circles()
        .resign_merge_journal_with_reference_for_test(&mut journal, reference, |_| {})
        .await
        .expect("re-sign Circle commit with substituted exact graph");
    let store_commit = journal.operation().commit_ref.object.clone();
    let store_head = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Merge operation carries a Store head")
        .clone();
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted signed Circle graph");

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local activation must verify every signed exact Circle edge");

    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
    assert!(!_home.contains_exact_object(&store_commit));
    assert!(!_home.contains_exact_object(&store_head));
}

#[tokio::test]
async fn local_circle_activation_rejects_another_circle_or_grant_anchor() {
    for wrong_grant in [false, true] {
        let db = open_test_db();
        let label = if wrong_grant {
            "circle-wrong-stream-grant"
        } else {
            "circle-wrong-stream-circle"
        };
        let (store, _home, signer, mut journal) = persist_merge_operation(&db, label).await;
        let commit = journal.commit().expect("parse Circle commit");
        let [reference] = commit.circle_controls() else {
            panic!("Circle commit carries one control")
        };
        let device = store
            .bind_device(&db, &signer)
            .await
            .expect("bind substituted Circle object Store");
        let mut writer = device
            .authorize_writer()
            .await
            .expect("authorize substituted Circle object Store");
        writer
            .circles()
            .resign_merge_journal_with_reference_for_test(
                &mut journal,
                reference.clone(),
                move |commit| {
                    let activations = match &mut commit.body_mut().body {
                        coven_protocol::store_commit::StoreCommitBody::Operations(operations) => {
                            &mut operations.stream_activations
                        }
                        _ => panic!("Circle commit body carries operations"),
                    };
                    let activation = activations
                        .iter_mut()
                        .find(|activation| {
                            matches!(
                                activation,
                                StreamActivation::GrantAuthorized {
                                    anchor: GrantStreamAnchor::CircleRoster { .. },
                                    ..
                                }
                            )
                        })
                        .expect("founder Circle commit activates its roster stream");
                    let StreamActivation::GrantAuthorized {
                        grant_id, anchor, ..
                    } = activation
                    else {
                        unreachable!()
                    };
                    if wrong_grant {
                        *grant_id = coven_protocol::membership::MembershipGrantId(
                            ObjectHash::digest(b"another Circle grant"),
                        );
                    } else {
                        let GrantStreamAnchor::CircleRoster { circle_id, .. } = anchor else {
                            unreachable!()
                        };
                        *circle_id = CircleId::from_bytes([99; 16]);
                    }
                    activations.sort();
                },
            )
            .await
            .expect("re-sign Circle commit with substituted stream authority");
        let store_commit = journal.operation().commit_ref.object.clone();
        let store_head = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("Merge operation carries a Store head")
            .clone();
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = journal.operation().commit_ref.coord;
        coven_database::StoreDatabase::new(&db.database)
            .substitute_circle_operation_for_test(journal.clone())
            .await
            .expect("persist Circle journal with substituted stream authority");

        store
            .bind_device(&db, &signer)
            .await
            .expect("bind Circle test Store")
            .resume_circle_operations()
            .await
            .expect_err("Circle stream activation must name its signed Circle and grant");
        assert_eq!(
            StoreDatabase::new(&db.database)
                .circle_control_activation_count_for_test(journal.circle_id())
                .await
                .expect("count circle activations"),
            0
        );
        assert!(coven_database::StoreDatabase::new(&db.database)
            .exact_materialized_ref(&stream_id.to_string(), sequence)
            .await
            .expect("read rejected Circle Store position")
            .is_none());
        assert!(!_home.contains_exact_object(&store_commit));
        assert!(!_home.contains_exact_object(&store_head));
    }
}

#[tokio::test]
async fn local_circle_activation_rejects_an_unexpected_acknowledgement() {
    let db = open_test_db();
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, "circle-unexpected-acknowledgement").await;
    let device = store
        .bind_device(&db, &signer)
        .await
        .expect("load acknowledgement Store");
    device
        .stage_acknowledgement(
            coven_protocol::store_commit::CommitFrontier::from_refs(
                coven_database::StoreDatabase::new(&db.database)
                    .materialized_frontier()
                    .await
                    .expect("read current Store frontier"),
            )
            .expect("materialized Merge frontier is typed"),
            "2026-07-19T00:00:00Z".to_string(),
        )
        .await
        .expect("stage a valid non-initial Store acknowledgement");
    let acknowledgement = coven_database::StoreDatabase::new(&db.database)
        .oldest_outbound_store_ack()
        .await
        .expect("read staged Store acknowledgement")
        .expect("staged Store acknowledgement remains queued")
        .reference;
    let commit = journal.commit().expect("parse Circle commit");
    let [reference] = commit.circle_controls() else {
        panic!("Circle commit carries one control")
    };
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize substituted Circle object Store");
    writer
        .circles()
        .resign_merge_journal_with_reference_for_test(
            &mut journal,
            reference.clone(),
            move |commit| {
                let coven_protocol::store_commit::StoreCommitBody::Operations(operations) =
                    &mut commit.body_mut().body
                else {
                    panic!("Circle commit body carries operations")
                };
                operations.acknowledgement = Some(acknowledgement);
            },
        )
        .await
        .expect("re-sign Circle commit with unexpected acknowledgement");
    let store_commit = journal.operation().commit_ref.object.clone();
    let store_head = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Merge operation carries a Store head")
        .clone();
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = journal.operation().commit_ref.coord;
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist Circle journal with unexpected acknowledgement");

    let error = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("Circle journal must contain no operation besides its control");
    assert!(error.to_string().contains("control-only batch"), "{error}");
    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
    assert!(coven_database::StoreDatabase::new(&db.database)
        .exact_materialized_ref(&stream_id.to_string(), sequence)
        .await
        .expect("read rejected Circle Store position")
        .is_none());
    assert!(!_home.contains_exact_object(&store_commit));
    assert!(!_home.contains_exact_object(&store_head));
}

#[tokio::test]
async fn local_successor_rejects_an_unreserved_circle_predecessor() {
    let db = open_test_db();
    let (store, _home, signer, founder) =
        persist_merge_operation(&db, "circle-unreserved-predecessor").await;
    let circle_id = founder.circle_id();
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("publish founder Circle");
    _home.fail_exact_create_before_call(1);
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle rename Store")
        .rename_circle("0000000002000-0000-creator", circle_id, "Renamed household")
        .await
        .expect_err("interrupt rename before its first exact upload");
    let operation_id = coven_database::StoreDatabase::new(&db.database)
        .get_circle_operations()
        .await
        .expect("list interrupted rename")
        .into_iter()
        .find(|operation| operation.circle_id == circle_id)
        .expect("interrupted rename remains pending")
        .operation_id;
    let mut journal = coven_database::StoreDatabase::new(&db.database)
        .circle_operation(&operation_id)
        .await
        .expect("read interrupted rename")
        .expect("interrupted rename journal remains durable");
    let commit = journal.commit().expect("parse rename commit");
    let author = coven_database::StoreDatabase::new(&db.database)
        .activated_store_device_registration(commit.author_registration.clone())
        .await
        .expect("load rename author");
    let device_signer = author
        .value()
        .device_signer(&signer)
        .expect("derive rename device signer");
    let original_slot = journal
        .operation()
        .prepared_objects
        .get("control-head")
        .expect("rename carries a control head")
        .slot()
        .clone();
    let creation = &mut journal.operation_mut().creation;
    let CircleTransitionPolicyObjects { control_head, .. } = &mut creation.policy_objects;
    control_head.body_mut().successor.predecessor =
        Some(coven_protocol::objects::ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(
                "store-v1/test-circle-controls/unreserved-predecessor.json".to_string(),
            )
            .expect("construct arbitrary predecessor slot"),
            1,
            ObjectHash::digest(b"unreserved Circle predecessor"),
        ));
    control_head.resign(&device_signer);
    let head_prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
        circle_id,
        control: &control_head.control,
    });
    let forging_device = store
        .bind_device(&db, &signer)
        .await
        .expect("bind forged Circle object Store");
    let prepared_head = forging_device
        .prepare_circle_object_at(
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            original_slot,
            &head_prefix,
            serde_json::to_vec(&control_head).expect("serialize forged control head"),
        )
        .await
        .expect("prepare forged control head");
    spool_substituted_object(&db.store_dir, &prepared_head).await;
    journal.operation_mut().prepared_objects.insert(
        "control-head".to_string(),
        prepared_head.reference().clone(),
    );
    let [old_reference] = commit.circle_controls() else {
        panic!("rename commit carries one Circle reference")
    };
    let reference = journal.operation().creation.control_ref(
        old_reference.objects().clone(),
        Some(prepared_head.reference().clone()),
    );
    let device = store
        .bind_device(&db, &signer)
        .await
        .expect("bind substituted Circle object Store");
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize substituted Circle object Store");
    writer
        .circles()
        .resign_merge_journal_with_reference_for_test(&mut journal, reference, |_| {})
        .await
        .expect("re-sign Circle commit with unreserved predecessor");
    let store_commit = journal.operation().commit_ref.object.clone();
    let store_head = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Merge operation carries a Store head")
        .clone();
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal)
        .await
        .expect("persist forged successor journal");

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("common verifier must reject an unreserved Circle predecessor");
    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(circle_id)
            .await
            .expect("count circle activations"),
        1
    );
    assert!(!_home.contains_exact_object(&store_commit));
    assert!(!_home.contains_exact_object(&store_head));
}

#[tokio::test]
async fn local_publication_rejects_a_store_head_outside_its_reserved_slot() {
    let db = open_test_db();
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, "circle-substituted-local-head-slot").await;
    let original = journal
        .operation()
        .prepared_objects
        .get("store-head")
        .expect("Merge operation carries an exact Store head");
    let substituted_slot = coven_protocol::objects::ObjectSlot::opaque(
        original.slot().logical_key().to_string(),
        "substituted-store-head".to_string(),
    )
    .expect("construct alternate Store head slot");
    // The head's own bytes, under a slot the Store never reserved for it.
    let substituted = coven_protocol::objects::ExactObjectRef::new(
        substituted_slot,
        original.stored_size(),
        original.stored_hash(),
    );
    journal
        .operation_mut()
        .prepared_objects
        .insert("store-head".to_string(), substituted);
    coven_database::StoreDatabase::new(&db.database)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted Store head slot");

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local publication must reject an unreserved Store head slot");

    assert_eq!(
        StoreDatabase::new(&db.database)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

/// A roster conflict resolution lives at `circles/…/roster/resolutions/…`, a path
/// the roster entry-and-head domain does not accept, so it seals and opens only
/// under its own domain — and its own domain accepts nothing else. That domain is
/// what separates the resolution from the entries and heads it is read alongside:
/// the associated data is `store root hash || domain label || semantic prefix`, so
/// no roster object of one kind opens where another kind is read, whatever slot its
/// bytes are moved to. Behind that, the two plaintexts have disjoint required
/// fields and refuse unknown ones, so a member who can seal — the one party the
/// sealing context does not stop — still cannot pass an entry off as a resolution.
#[tokio::test]
async fn a_roster_resolution_seals_and_opens_only_under_its_own_domain() {
    let db = open_test_db();
    let (fixture, _home, signer, journal) =
        persist_merge_operation_fixture(&db, "circle-roster-kind-crossing").await;
    let store = fixture.store;
    let cloud_storage = fixture.storage;
    let author = keys::public_key_hex(&signer);
    let access = journal
        .operation()
        .creation
        .access
        .iter()
        .find(|access| access.leaf.value.recipient_pubkey == author)
        .expect("founder access");
    let CircleAccessDisposition::Active { keyring, .. } = &access.leaf.value.disposition else {
        panic!("founder access must be active")
    };
    let encryption = EncryptionService::from(
        MasterKeyring::from_serialized(keyring).expect("parse founder Circle keyring"),
    );
    let circle_id = journal.circle_id();
    let commit = journal.commit().expect("parse Circle commit");
    let store_root_hash = commit.store_root_hash;
    let entry_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRoster,
        encryption.clone(),
    );
    let resolution_context = ProtocolObjectContext::circle(
        store_root_hash,
        ProtocolObjectDomain::CircleRosterResolution,
        encryption.clone(),
    );
    let [control] = commit.circle_controls() else {
        panic!("Circle operation must carry one control")
    };
    let (entry_coord, entry_object) = control
        .objects()
        .roster_entries
        .iter()
        .next()
        .map(|(coord, object)| (coord.clone(), object.clone()))
        .expect("founder graph carries a roster entry");
    let entry_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
        circle_id,
        coord: &entry_coord,
    });
    let entry_reference = journal
        .operation()
        .prepared_objects
        .get("roster-entry")
        .expect("the operation carries its sealed roster entry");
    assert_eq!(entry_reference, &entry_object);
    store
        .publish_exact_protocol_object(
            entry_reference,
            spooled_bytes(&db.store_dir, entry_reference).await,
        )
        .await
        .expect("publish the founder roster entry");
    let entry_plaintext = cloud_storage
        .read_protocol_object(&entry_context, &entry_object, &entry_prefix)
        .await
        .expect("open the founder roster entry where it belongs");

    let resolution =
        coven_protocol::circle_roster::CircleRosterConflictResolution::unsigned_for_test(
            coven_protocol::circle_roster::CircleRosterConflictResolutionBody {
                store_root_hash,
                circle_id,
                conflict_hash: ObjectHash::digest(b"roster kind crossing conflict"),
                conflicting_heads: Vec::new(),
                retired_owner_grants: Default::default(),
                resolver_pubkey: author.clone(),
                resolver_branch_heads: Vec::new(),
                replacement_grant: coven_protocol::membership::MembershipGrantId(
                    ObjectHash::digest(b"roster kind crossing grant"),
                ),
            },
        );
    let resolution_plaintext =
        serde_json::to_vec(&resolution).expect("serialize the roster resolution");
    let resolution_ref = coven_protocol::circle_roster::CircleRosterConflictResolutionRef {
        conflict_hash: resolution.conflict_hash,
        resolver_pubkey: author,
        resolution_hash: ObjectHash::digest(&resolution_plaintext),
    };
    let resolution_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterResolution {
        circle_id,
        resolution: &resolution_ref,
    });
    let resolution_slot = cloud_storage
        .allocate_protocol_slot(&resolution_context, &resolution_prefix, ".json")
        .await
        .expect("allocate the roster resolution slot");
    let resolution_object = cloud_storage
        .prepare_protocol_object(
            &resolution_context,
            resolution_slot,
            &resolution_prefix,
            resolution_plaintext.clone(),
        )
        .expect("seal the roster resolution");
    cloud_storage
        .create_protocol_object(&resolution_object)
        .await
        .expect("publish the roster resolution");
    assert_eq!(
        cloud_storage
            .read_protocol_object(
                &resolution_context,
                resolution_object.reference(),
                &resolution_prefix
            )
            .await
            .expect("open the roster resolution where it belongs"),
        resolution_plaintext,
    );

    // Neither domain accepts the other's path at all.
    cloud_storage
        .allocate_protocol_slot(&entry_context, &resolution_prefix, ".json")
        .await
        .expect_err("the roster entry domain must refuse a resolution path");
    cloud_storage
        .allocate_protocol_slot(&resolution_context, &entry_prefix, ".json")
        .await
        .expect_err("the roster resolution domain must refuse an entry path");

    // And each kind's sealed bytes, moved into the other kind's slot, no longer
    // match the associated data they were sealed under.
    for (sealed_from, target_context, target_prefix) in [
        (
            entry_object.slot().clone(),
            &resolution_context,
            resolution_prefix.clone(),
        ),
        (
            resolution_object.reference().slot().clone(),
            &entry_context,
            entry_prefix.clone(),
        ),
    ] {
        let sealed = _home
            .stored_exact_bytes(&sealed_from)
            .expect("read the sealed bytes to move");
        let moved_slot =
            _home.insert_exact_object(&format!("{target_prefix}.json"), sealed.clone());
        let moved =
            ExactObjectRef::new(moved_slot, sealed.len() as u64, ObjectHash::digest(&sealed));
        let error = cloud_storage
            .read_protocol_object(target_context, &moved, &target_prefix)
            .await
            .expect_err("sealed roster bytes must not open under another kind");
        assert!(
            matches!(error, coven_protocol::objects::StorageError::Decryption(_)),
            "moving roster bytes across kinds must fail the sealing context: {error}",
        );
    }

    // The plaintexts themselves, for the member who seals correctly.
    serde_json::from_slice::<coven_protocol::circle_roster::CircleRosterConflictResolution>(
        &entry_plaintext,
    )
    .expect_err("a roster entry must not parse as a roster resolution");
    serde_json::from_slice::<coven_protocol::circle_roster::CircleRosterEntry>(
        &resolution_plaintext,
    )
    .expect_err("a roster resolution must not parse as a roster entry");
}
