use super::*;
#[tokio::test]
async fn local_activation_rejects_sealed_leaf_plaintext_substitution() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-mismatched-local-keyring").await;
    let author = keys::public_key_hex(&signer);
    let own_access = journal
        .operation_mut()
        .creation
        .access
        .iter_mut()
        .find(|access| access.value.recipient_pubkey == author)
        .expect("founder access");
    let CircleAccessDisposition::Active { keyring, .. } =
        &mut own_access.value.body_mut().disposition
    else {
        panic!("founder access must be active")
    };
    *keyring = MasterKeyring::generate().to_serialized();
    own_access.value.resign(&signer);
    coven_database::StoreDatabase::new(&db)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted journal plaintext");
    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local activation must reject substituted journal plaintext");
    assert!(
        error
            .to_string()
            .contains("prepared Circle access leaf differs from its signed control entry"),
        "{error}"
    );
    assert_eq!(
        StoreDatabase::new(&db)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

#[tokio::test]
async fn local_publication_rejects_a_prepared_object_outside_the_signed_graph() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, mut journal) = persist_merge_operation(
        &db,
        db_store_dir.clone(),
        "circle-substituted-local-object-ref",
    )
    .await;
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
    // Only the slot moves: the same bytes are already stored under the same
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
    coven_database::StoreDatabase::new(&db)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted journal object");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local publication must reject objects outside the signed graph");
    assert!(
        error
            .to_string()
            .contains("outside its signed Store commit graph"),
        "{error}"
    );

    assert_eq!(
        StoreDatabase::new(&db)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

#[tokio::test]
async fn local_activation_rejects_substituted_exact_circle_edges() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, mut journal) =
        persist_merge_operation(&db, db_store_dir.clone(), "circle-substituted-signed-edges").await;
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
        let metadata_object = std::mem::replace(&mut metadata.object, roster.object);
        objects.roster_entries.insert(
            roster_coord,
            coven_protocol::store_commit::CircleRosterEntryRef {
                object: metadata_object,
                origin: roster.origin,
            },
        );
    }
    let reference = journal.operation().creation.control_ref(objects);
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
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
    let store_commit = journal.operation().commit_ref().object.clone();
    let publication = journal
        .operation()
        .store_commit
        .publication
        .entry_object
        .clone();
    coven_database::StoreDatabase::new(&db)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted signed Circle graph");

    store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("local activation must verify every signed exact Circle edge");

    assert_eq!(
        StoreDatabase::new(&db)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
    assert!(!_home.contains_exact_object(&store_commit));
    assert!(!_home.contains_exact_object(&publication));
}

#[tokio::test]
async fn local_circle_activation_rejects_an_unexpected_acknowledgement() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, mut journal) = persist_merge_operation(
        &db,
        db_store_dir.clone(),
        "circle-unexpected-acknowledgement",
    )
    .await;
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("load acknowledgement Store");
    device
        .stage_acknowledgement(
            coven_protocol::store_commit::CommitFrontier::from_refs(
                coven_database::StoreDatabase::new(&db)
                    .materialized_frontier()
                    .await
                    .expect("read current Store frontier"),
            )
            .expect("materialized Merge frontier is typed"),
            "2026-07-19T00:00:00Z".to_string(),
        )
        .await
        .expect("stage a valid non-initial Store acknowledgement");
    let acknowledgement = coven_database::StoreDatabase::new(&db)
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
    let store_commit = journal.operation().commit_ref().object.clone();
    let publication = journal
        .operation()
        .store_commit
        .publication
        .entry_object
        .clone();
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = journal.operation().commit_ref().coord;
    coven_database::StoreDatabase::new(&db)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist Circle journal with unexpected acknowledgement");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err("Circle journal must contain no operation besides its control");
    assert!(error.to_string().contains("control-only batch"), "{error}");
    assert_eq!(
        StoreDatabase::new(&db)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
    assert!(coven_database::StoreDatabase::new(&db)
        .exact_materialized_ref(&stream_id.to_string(), sequence)
        .await
        .expect("read rejected Circle Store position")
        .is_none());
    assert!(!_home.contains_exact_object(&store_commit));
    assert!(!_home.contains_exact_object(&publication));
}

#[tokio::test]
async fn local_publication_rejects_a_substituted_publication_entry_slot() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let (store, _home, signer, mut journal) = persist_merge_operation(
        &db,
        db_store_dir.clone(),
        "circle-substituted-publication-entry-slot",
    )
    .await;
    let original = &journal.operation().store_commit.publication.entry_object;
    let substituted_slot = coven_protocol::objects::ObjectSlot::opaque(
        original.slot().logical_key().to_string(),
        "substituted-publication-entry".to_string(),
    )
    .expect("construct alternate publication entry slot");
    // The same entry bytes at a slot its signed current-record replacement does not name.
    let substituted = coven_protocol::objects::ExactObjectRef::new(
        substituted_slot,
        original.stored_size(),
        original.stored_hash(),
    );
    journal
        .operation_mut()
        .store_commit
        .publication
        .entry_object = substituted;
    coven_database::StoreDatabase::new(&db)
        .substitute_circle_operation_for_test(journal.clone())
        .await
        .expect("persist substituted publication entry slot");

    let error = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect_err(
            "local publication must reject an entry slot different from its signed replacement",
        );
    assert!(
        error
            .to_string()
            .contains("prepared Store publication differs from its commit or predecessor"),
        "{error}"
    );

    assert_eq!(
        StoreDatabase::new(&db)
            .circle_control_activation_count_for_test(journal.circle_id())
            .await
            .expect("count circle activations"),
        0
    );
}

/// A published founder Circle plus a rename journaled but not yet published, so
/// a test can re-sign its signed object graph and watch the local candidate
/// refuse it before anything reaches the cloud.
///
/// These two tests stand where the head-slot ones stood: a Circle successor
/// used to be pinned to its stream by a predecessor-reserved create-once slot
/// and by the Circle-and-grant anchor its stream activation named. Both are
/// gone, and what pins a successor now is its entry provenance — it carries
/// every entry each covered predecessor published, and every inherited entry
/// names the exact accepted activation that introduced it.
struct JournaledSuccessor {
    db: Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<TestStore>,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    signer: UserKeypair,
    journal: CircleOperationJournal,
    another_circles_activation: coven_protocol::store_commit::StoreBatchCommitRef,
}

impl JournaledSuccessor {
    async fn build(label: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let (store, home, signer, founder_journal) =
            persist_merge_operation(&db, db_store_dir.clone(), label).await;
        let circle_id = founder_journal.circle_id();
        store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Circle publishing Store")
            .resume_circle_operations()
            .await
            .expect("publish the founder Circle");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Circle authoring Store");
        let another_circle = device
            .create_circle("0000000001500-0000-creator", "Allotment")
            .await
            .expect("create a second Circle in the same Store");
        let identity_pubkey = keys::public_key_hex(&signer);
        let database = StoreDatabase::new(&db);
        let (_, another_circles_activation) = database
            .circle_authoring_context(another_circle, &identity_pubkey)
            .await
            .expect("read the second Circle's authoring context");
        let (current, activation_commit_ref) = database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await
            .expect("read the founder authoring context");
        let activation_commit = device
            .load_commit_for_test(&activation_commit_ref)
            .await
            .expect("load the founder activating commit");
        let previous_control = coven_protocol::circle_journal::CircleControlActivation {
            reference: activation_commit
                .value()
                .circle_controls()
                .iter()
                .find(|reference| {
                    reference.circle_id() == circle_id
                        && reference.control() == &current.control.coord
                })
                .expect("the founder control is present in its activating commit")
                .clone(),
            activating_commit: activation_commit_ref,
        };
        let mut authority = device
            .authorize_writer()
            .await
            .expect("authorize Circle successor writer");
        let prepared = authority
            .circles()
            .preparer()
            .prepare_request(CircleOperationRequest::Rename(Box::new(
                super::commands::CircleRenameRequest {
                    circle_id,
                    name: "Cottage".to_string(),
                    metadata_stamp: "0000000002000-0000-creator".to_string(),
                    current,
                    previous_control,
                },
            )))
            .await
            .expect("prepare the Circle rename");
        drop(authority);
        database
            .insert_circle_operation(prepared.journal.clone(), prepared.prepared_objects)
            .await
            .expect("journal the rename before publication");
        Self {
            db,
            db_store_dir,
            store,
            home,
            signer,
            journal: prepared.journal,
            another_circles_activation,
        }
    }

    /// Re-sign the journaled rename around a tampered object graph, resume it,
    /// and report the local refusal together with what never reached the cloud.
    async fn local_refusal(
        mut self,
        tamper: impl FnOnce(&mut coven_protocol::store_commit::CircleActivationObjects),
    ) -> CircleOperationError {
        let commit = self
            .journal
            .commit()
            .expect("parse the prepared rename commit");
        let [reference] = commit.circle_controls() else {
            panic!("a Circle rename commit carries one control reference")
        };
        let mut objects = reference.objects().clone();
        tamper(&mut objects);
        let reference = self.journal.operation().creation.control_ref(objects);
        let device = self
            .store
            .bind_device_in(&self.db, self.db_store_dir.clone(), &self.signer)
            .await
            .expect("bind tampered Circle object Store");
        let mut writer = device
            .authorize_writer()
            .await
            .expect("authorize tampered Circle object Store");
        writer
            .circles()
            .resign_merge_journal_with_reference_for_test(&mut self.journal, reference, |_| {})
            .await
            .expect("re-sign the rename commit around the tampered graph");
        drop(writer);
        drop(device);
        let store_commit = self.journal.operation().commit_ref().object.clone();
        let publication = self
            .journal
            .operation()
            .store_commit
            .publication
            .entry_object
            .clone();
        StoreDatabase::new(&self.db)
            .substitute_circle_operation_for_test(self.journal.clone())
            .await
            .expect("persist the tampered signed Circle graph");

        let error = self
            .store
            .bind_device_in(&self.db, self.db_store_dir.clone(), &self.signer)
            .await
            .expect("bind Circle test Store")
            .resume_circle_operations()
            .await
            .expect_err("the local candidate must refuse its own tampered successor");
        assert!(!self.home.contains_exact_object(&store_commit));
        assert!(!self.home.contains_exact_object(&publication));
        error
    }
}

/// A successor that drops an entry its covered predecessor published is refused
/// by the device that prepared it, before its commit is uploaded.
#[tokio::test]
async fn local_successor_rejects_a_graph_that_drops_a_covered_entry() {
    let fixture = JournaledSuccessor::build("circle-local-successor-drops-entry").await;

    let error = fixture
        .local_refusal(|objects| {
            let coord = objects
                .roster_entries
                .keys()
                .next()
                .cloned()
                .expect("the rename inherits the founder roster entry");
            objects.roster_entries.remove(&coord);
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("Circle control drops or replaces an entry a covered predecessor published"),
        "{error}"
    );
}

/// The entry this successor introduces cannot be claimed as inherited from
/// another Circle's activating commit: that commit activates no control for
/// this Circle, so it introduced nothing the entry can rest on. This is what
/// the stream activation's Circle-and-grant anchor used to say, now said about
/// the entry's own place in accepted history.
#[tokio::test]
async fn local_successor_rejects_an_entry_inherited_from_another_circle() {
    let fixture = JournaledSuccessor::build("circle-local-successor-foreign-origin").await;
    let another = fixture.another_circles_activation.clone();

    let error = fixture
        .local_refusal(|objects| {
            let introduced = objects
                .metadata_entries
                .iter()
                .find(|(_, reference)| reference.origin.is_introduced())
                .map(|(coord, _)| coord.clone())
                .expect("the rename introduces its own metadata entry");
            objects
                .metadata_entries
                .get_mut(&introduced)
                .expect("the introduced metadata entry")
                .origin = coven_protocol::store_commit::CircleEntryOrigin::Inherited {
                activating_commit: another.clone(),
            };
        })
        .await;

    assert!(
        error
            .to_string()
            .contains("which activates no control for it"),
        "{error}"
    );
}
