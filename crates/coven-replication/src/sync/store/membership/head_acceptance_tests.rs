use super::*;

#[tokio::test]
async fn membership_preparation_waits_for_its_predecessors_accepted_result() {
    let fixture = MergeFixture::new("membership-predecessor-result").await;
    let peer_directory = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_directory.clone());
    let peer = fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &peer_db,
            peer_directory.clone(),
            &fixture.owner,
            "2026-09-09T00:00:00Z",
        )
        .await
        .expect("activate another device on the Owner membership stream");
    let predecessor = fixture.load().await;
    let tip = predecessor.head_refs().last().expect("Owner head");
    let prior_head = fixture
        .device
        .load_membership_head_for_test(tip)
        .await
        .expect("load predecessor head");
    let first = pubkey_hex(&UserKeypair::generate());
    let second = pubkey_hex(&UserKeypair::generate());
    let encryption = EncryptionService::from_key([42; 32]);
    let (accepted, release) = fixture.home.pause_next_conditional_replace();
    let mut admission = Box::pin(fixture.store.admit_member(
        &fixture.db,
        fixture.store_dir.clone(),
        &fixture.owner,
        &first,
        None,
        MemberRole::Member,
        &encryption,
        "Test Store",
    ));
    tokio::select! {
        _ = accepted.notified() => {},
        result = &mut admission => panic!("admission ended before accepted publication: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("admission never reached acceptance"),
    }
    fixture.home.fail_exact_create_before_call(1);
    release.notify_one();
    let failure = admission
        .await
        .expect_err("interrupt accepted result upload");
    assert!(
        failure
            .to_string()
            .contains("forced failure before exact create call 1"),
        "{failure}"
    );
    let bytes = fixture
        .home
        .stored_exact_bytes(&prior_head.body.successor.next_slot)
        .expect("the accepted control's membership head was uploaded");
    let head: AuthorHead = coven_protocol::objects::decode_protocol_object(&bytes)
        .expect("decode accepted membership head");
    let coven_protocol::membership::MembershipHeadActivation::StoreCommit {
        acceptance_slot,
        commit,
    } = &head.activation
    else {
        panic!("the control requires its Store acceptance result");
    };
    let active = fixture
        .database
        .active_store_publication()
        .await
        .expect("read accepted pending publication")
        .expect("origin retains finalization");
    assert_eq!(
        active.commit_reservation().expect("reserved commit").2,
        &commit.coord
    );
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_none());

    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer installs the accepted control");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('while-finalizing', 'peer edit', 1, '0000000001000-0000-peer', '2026-09-09')",
        )
        .await;
    assert!(peer
        .publish_pending_store_database()
        .await
        .expect("ordinary peer writes remain eligible"));
    let peer_database = StoreDatabase::new(&peer_db);
    let rejected = fixture
        .store
        .admit_member(
            &peer_db,
            peer_directory.clone(),
            &fixture.owner,
            &second,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect_err("a successor cannot be prepared before its predecessor result exists");
    assert!(rejected.to_string().contains("finalization"), "{rejected}");
    assert!(peer_database
        .outbound_membership_mutation()
        .await
        .expect("peer journal")
        .is_none());
    assert!(peer_database
        .active_store_publication()
        .await
        .expect("peer reservation")
        .is_none());
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_none());

    fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.owner,
            &first,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("origin finalizes its exact accepted control");
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_some());
    let resumed = fixture
        .store
        .admit_member(
            &peer_db,
            peer_directory,
            &fixture.owner,
            &second,
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("prepare and publish after predecessor finalization");
    assert!(resumed
        .membership_floor
        .0
        .iter()
        .any(
            |reference| reference.coord.stream_key() == head.entry_coord().stream_key()
                && reference.coord.seq == head.entry_coord().seq + 1
        ));
}

#[tokio::test]
async fn a_rollup_cannot_substitute_another_signed_predecessor_result() {
    let fixture = MergeFixture::new("rollup-predecessor-result").await;
    fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    let admission = fixture
        .admit_member(&UserKeypair::generate(), MemberRole::Member)
        .await;
    let snapshot = fixture
        .device
        .publish_snapshot_generation_for_test()
        .await
        .expect("publish accepted membership rollup");
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipRollup,
    );
    let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
        &snapshot.meta.membership_rollup.object,
        ".json",
    )
    .expect("rollup prefix");
    let bytes = fixture
        .storage
        .read_protocol_object(&context, &snapshot.meta.membership_rollup.object, &prefix)
        .await
        .expect("read actual published rollup");
    let mut rollup: coven_protocol::store_commit::MembershipRollup =
        coven_protocol::objects::decode_protocol_object(&bytes).expect("decode rollup");
    let registration = fixture
        .database
        .activated_store_device_registration(rollup.author_registration.clone())
        .await
        .expect("actual publisher registration");
    let signer = registration
        .value()
        .device_signer(&fixture.owner)
        .expect("actual device signer");
    let stream = rollup
        .body_mut()
        .streams
        .iter_mut()
        .find(|stream| stream.author_pubkey == fixture.owner_pubkey)
        .expect("Owner stream");
    let previous_head = stream.heads[stream.heads.len() - 2].head_value.clone();
    let last = stream.heads.last_mut().expect("terminal covered head");
    let result = last
        .predecessor_acceptance
        .as_mut()
        .expect("accepted predecessor result");
    assert!(!result.accepted_predecessor.0.is_empty());
    let selected_head = result.head.clone();
    result.body_mut().accepted_predecessor =
        coven_protocol::membership::MembershipFloor(vec![selected_head]);
    result.resign(&signer);
    result
        .verify_for(
            fixture.store.root().store_root_hash,
            &result.head,
            &previous_head,
            registration.value(),
        )
        .expect("alternate result still has a valid issuer signature and exact head");
    let coven_protocol::membership::MembershipHeadPredecessor::Accepted { acceptance, .. } = last
        .head_value
        .body_mut()
        .body
        .predecessor
        .as_mut()
        .expect("predecessor")
    else {
        panic!("Store predecessor needs its accepted result");
    };
    assert!(acceptance.verify(&result.to_bytes()).is_err());
    let replacement = result.to_bytes();
    *acceptance = ExactObjectRef::new(
        acceptance.slot().clone(),
        replacement.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&replacement),
    );
    last.head_value.resign(&signer);
    last.head.head_hash = last.head_value.head_hash();
    let replacement_head = last.head_value.to_bytes();
    last.head.object = ExactObjectRef::new(
        last.head.object.slot().clone(),
        replacement_head.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&replacement_head),
    );
    rollup.resign(&signer);
    rollup
        .validate_shape()
        .expect("alternate rollup has internally valid exact objects");

    let expected = walk_admission_membership(&fixture, &admission, false).await;
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(&*fixture.storage, &admission.store_root)
        .await
        .expect("independent cold reader");
    history
        .admit_membership_rollup(&rollup)
        .await
        .expect("admit signed advisory objects");
    let actual = history
        .load_accepted_anchored_membership(
            &admission.membership_floor.0,
            Some(&admission.owner_pubkey),
        )
        .await
        .expect("provider-pinned successor selects its genuine predecessor result");
    assert_eq!(actual.head_refs(), expected.head_refs());
    assert_eq!(actual.effective_frontier(), expected.effective_frontier());
}

#[tokio::test]
async fn an_unaccepted_membership_head_does_not_require_its_commit_to_be_uploaded() {
    // The publisher stays in this future: timeout or assertion failure drops
    // the paused operation and releases its permits without a detached task.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        use crate::sync::test_helpers::{InterceptedStorage, StorageInterceptor};
        use coven_protocol::objects::{PreparedExactObject, StorageError};
        use tokio::sync::{oneshot, Notify};

        type PauseNotifications = (Arc<Notify>, Arc<Notify>);

        struct PauseHeadCreation {
            home: Arc<coven_storage::InMemoryCloudHome>,
            slot: ObjectSlot,
            armed: std::sync::Mutex<Option<oneshot::Sender<PauseNotifications>>>,
        }

        #[async_trait::async_trait]
        impl StorageInterceptor for PauseHeadCreation {
            async fn before_protocol_create(
                &self,
                prepared: &PreparedExactObject,
            ) -> Result<(), StorageError> {
                if prepared.reference().slot() == &self.slot {
                    let sender = self.armed.lock().unwrap().take().expect("head created once");
                    let pause = self.home.pause_after_exact_create_call(1);
                    assert!(sender.send(pause).is_ok(), "head observer remains alive");
                }
                Ok(())
            }
        }

        let fixture = MergeFixture::new("membership-head-before-commit").await;
        let peer_directory = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_directory.clone());
        let peer = fixture.store.activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &peer_db,
            peer_directory,
            &fixture.owner,
            "2026-09-09T00:00:00Z",
        ).await.expect("activate the independent peer");
        let (_, initial_pull) = peer.pull_store().await.expect("catch the peer up");
        assert!(initial_pull.held_positions.is_empty(), "{initial_pull:?}");
        let before_membership = peer.membership_for_test().await.expect("peer membership");
        let peer_database = StoreDatabase::new(&peer_db);
        let before_boundary = peer_database.store_publication_boundary().await
            .expect("read peer boundary").expect("peer has accepted history");
        before_boundary.require_observed().expect("peer observed the provider");
        let current_slot = fixture.device.protocol_root_for_test()
            .descriptor.current_publication_slot.clone();
        let before_current = fixture.home.stored_exact_bytes(&current_slot)
            .expect("current publication exists");
        let current: coven_protocol::store_commit::StoreCurrentPublicationRecord =
            coven_protocol::objects::decode_protocol_object(&before_current)
                .expect("decode current publication");
        assert_eq!(before_boundary.record(), &current);

        let predecessor = fixture.load().await;
        assert_eq!(before_membership.head_refs(), predecessor.head_refs());
        let tip = predecessor.head_refs().last().expect("Owner stream head");
        let prior_head = fixture.device.load_membership_head_for_test(tip).await
            .expect("read the exact predecessor head");
        let head_slot = prior_head.body.successor.next_slot.clone();
        assert!(fixture.home.stored_exact_bytes(&head_slot).is_none());
        let (armed_sender, armed_receiver) = oneshot::channel();
        let storage = Arc::new(InterceptedStorage::new(
            fixture.storage.clone(),
            PauseHeadCreation {
                home: fixture.home.clone(),
                slot: head_slot.clone(),
                armed: std::sync::Mutex::new(Some(armed_sender)),
            },
        ));
        let store = fixture.store.open_store_with_storage(
            fixture.database.clone(), storage, fixture.store_dir.clone(), &fixture.owner,
        ).await.expect("open the real admission owner with intercepted storage");
        let member = pubkey_hex(&UserKeypair::generate());
        let encryption = EncryptionService::from_key([42; 32]);
        fixture.home.clear_exact_creates();
        let mut admission = Box::pin(store.admit_member(
            &member, None, MemberRole::Member, &encryption, &fixture.store_id, "Test Store",
        ));
        let (reached, release) = tokio::select! {
            pause = armed_receiver => pause.expect("head creation armed its pause"),
            result = &mut admission => panic!("admission ended before head creation: {result:?}"),
        };
        tokio::select! {
            _ = reached.notified() => {},
            result = &mut admission => panic!("admission ended before the head became visible: {result:?}"),
        }
        let bytes = fixture.home.stored_exact_bytes(&head_slot)
            .expect("paused head is physically present");
        let head: AuthorHead = coven_protocol::objects::decode_protocol_object(&bytes)
            .expect("decode actual membership head");
        let head_ref = MembershipHeadRef {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
            object: ExactObjectRef::new(
                head_slot, bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(&bytes),
            ),
        };
        let coven_protocol::membership::MembershipHeadActivation::StoreCommit {
            commit, acceptance_slot,
        } = &head.activation else {
            panic!("admission head requires accepted Store publication");
        };
        assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_none());
        assert_eq!(fixture.home.stored_exact_bytes(&current_slot), Some(before_current.clone()));
        let active = fixture.database.active_store_publication().await
            .expect("read active publication").expect("admission retains its reservation");
        assert_eq!(active.commit_reservation().expect("reserved commit").2, &commit.coord);

        fixture.home.clear_exact_reads();
        let cold = crate::sync::store::HistoryConstructionAuthority::admission()
            .open_pinned(&*fixture.storage, &fixture.store.root()).await
            .expect("open independent cold authority");
        let error = cold.load_accepted_anchored_membership(
            predecessor.head_refs(), Some(&fixture.owner_pubkey),
        ).await.expect_err("an unfinished active head cannot authorize cold discovery");
        match error {
            AnchoredChainError::IncompleteFinalization { head, source: StorageError::NotFound(_) } => {
                assert_eq!(*head, head_ref);
            }
            error => panic!("expected exact unfinished head, got {error:?}"),
        }
        assert!(!fixture.home.exact_reads().contains(commit.object.slot()));
        fixture.home.clear_exact_reads();
        let (_, pulled) = peer.pull_store().await
            .expect("retained accepted history remains pullable");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert_eq!(pulled.frontier, initial_pull.frontier);
        assert_eq!(peer_database.store_publication_boundary().await.unwrap(), Some(before_boundary));
        let peer_membership = peer.membership_for_test().await.expect("read accepted peer membership");
        assert_eq!(peer_membership.head_refs(), before_membership.head_refs());
        assert_eq!(peer_membership.current_members(), before_membership.current_members());
        assert!(!peer_membership.can_write_now(&member));
        assert!(!fixture.home.exact_reads().contains(commit.object.slot()));
        assert_eq!(fixture.home.stored_exact_bytes(&current_slot), Some(before_current));
        assert!(fixture.home.stored_exact_bytes(commit.object.slot()).is_none(),
            "the shared publisher uploads the commit after its provisional membership head");

        release.notify_one();
        let admitted = admission.await.expect("finish the exact admission");
        assert!(admitted.membership_floor.0.contains(&head_ref));
        assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_some());
        assert!(fixture.home.stored_exact_bytes(commit.object.slot()).is_some());
        assert_eq!(fixture.home.exact_creates().iter()
            .filter(|slot| *slot == commit.object.slot()).count(), 1);
        assert!(fixture.database.active_store_publication().await.unwrap().is_none());
        assert!(fixture.database.outbound_membership_mutation().await.unwrap().is_none());
        let accepted = walk_admission_membership(&fixture, &admitted, false).await;
        assert!(accepted.can_write_now(&member));
        let (_, pulled) = peer.pull_store().await.expect("peer installs the completed admission");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        let peer_membership = peer.membership_for_test().await.expect("peer accepted membership");
        assert_eq!(peer_membership.head_refs(), accepted.head_refs());
        assert_eq!(peer_membership.current_members(), accepted.current_members());
    }).await.expect("membership head publication completes within the test deadline");
}
