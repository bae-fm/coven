use super::*;

/// Prepare a founder Circle operation on a member device, then revoke that
/// member's grant so publishing it blocks under lost authority.
struct RevokedOperation {
    db: Database,
    owner_db: Database,
    store: std::sync::Arc<TestStore>,
    founder: UserKeypair,
    successor: UserKeypair,
    operation_id: CircleOperationId,
    author_grant_id: crate::protocol::membership::MembershipGrantId,
}

impl RevokedOperation {
    async fn prepare(name: &str) -> Self {
        let db = open_test_db();
        let founder = UserKeypair::generate();
        let store = create_test_store_in_its_own_task(
            &db,
            name,
            &founder,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let successor = UserKeypair::generate();
        let successor_pubkey = keys::public_key_hex(&successor);
        let encryption = EncryptionService::from_key([42; 32]);
        store
            .invite_member(
                &db,
                &founder,
                &successor_pubkey,
                None,
                MemberRole::Member,
                &encryption,
                "Recovery test Store",
            )
            .await
            .expect("invite successor member");
        let successor_db = open_test_db();
        store
            .activate_joined_device(
                &db,
                &successor_db,
                &successor,
                "0000000001003-0000-successor",
            )
            .await
            .expect("activate successor device");
        let exact_membership = store
            .bind_device(&successor_db, &successor)
            .await
            .expect("bind successor Store")
            .membership_for_test()
            .await
            .expect("load the operation author's exact Store grant");
        let [author_grant_id] = exact_membership
            .active_grant_ids(&successor_pubkey)
            .into_iter()
            .collect::<Vec<_>>()
            .try_into()
            .expect("operation author has one active Store grant");
        // The member device is the one that authors and publishes; drive everything
        // through its database so its local membership view governs authority.
        let journal = store
            .bind_device(&successor_db, &successor)
            .await
            .expect("bind Circle preparation Store")
            .prepare_circle_operation("0000000001003-0000-successor", "Revoked Circle")
            .await
            .expect("prepare operation while authorized");
        let operation_id = journal.operation_id.clone();
        StoreDatabase::new(&successor_db)
            .insert_circle_operation(journal)
            .await
            .expect("persist operation");

        let custody = TestCustody::default();
        custody.set_initial_key([42; 32]);
        let security = crate::sync::test_helpers::test_store_security(
            "circle-recovery-test",
            std::sync::Arc::new(custody),
        );
        store
            .remove_member(&db, &founder, &successor_pubkey, &encryption, &security)
            .await
            .expect("remove successor grant");

        Self {
            db: successor_db,
            owner_db: db,
            store,
            founder,
            successor,
            operation_id,
            author_grant_id,
        }
    }
}

/// The operation-inspection surface (`Circles::operations`) reports a blocked
/// operation's full shape — id, circle, intent kind, and the typed
/// `AuthorityLost` block — and `retry` refuses an operation that is not blocked
/// with the typed `NotBlocked`.
#[tokio::test]
async fn operation_inspection_surface_reports_the_typed_block() {
    let revoked = RevokedOperation::prepare("recovery-inspection-surface").await;
    revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation without failing");

    let operations = StoreDatabase::new(&revoked.db)
        .get_circle_operations()
        .await
        .expect("read the operation-inspection surface");
    let info = operations
        .iter()
        .find(|info| info.operation_id == revoked.operation_id)
        .expect("the blocked operation is inspectable");
    assert_eq!(
        info.kind,
        crate::protocol::circle::CircleOperationKind::Create
    );
    assert_eq!(
        info.state,
        CircleOperationState::Blocked {
            block: crate::protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: revoked.author_grant_id.clone(),
            },
        },
    );

    // Retrying an operation that is not blocked is refused with the typed reason
    // the public `NotBlocked` error carries.
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        "recovery-inspection-notblocked",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let journal = store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Ready Circle")
        .await
        .expect("prepare a ready operation");
    let ready_id = journal.operation_id.clone();
    crate::database::StoreDatabase::new(&db)
        .insert_circle_operation(journal)
        .await
        .expect("persist the ready operation");
    let refusal = store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&ready_id)
        .await
        .expect_err("a non-blocked operation is not retriable");
    assert!(
        matches!(&refusal, CircleOperationError::NotBlocked { operation_id } if *operation_id == ready_id),
        "{refusal:?}"
    );
}

#[tokio::test]
async fn a_blocked_operation_reports_typed_authority_lost() {
    let revoked = RevokedOperation::prepare("recovery-typed-block").await;
    revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation without failing");
    let blocked = StoreDatabase::new(&revoked.db)
        .circle_operation(&revoked.operation_id)
        .await
        .expect("read blocked operation")
        .expect("blocked operation remains durable");
    assert_eq!(
        blocked.state(),
        CircleOperationState::Blocked {
            block: crate::protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: revoked.author_grant_id.clone(),
            },
        },
        "the block names the author's exact grant"
    );
    // Surfaced typed through the query API.
    let operations = StoreDatabase::new(&revoked.db)
        .get_circle_operations()
        .await
        .expect("read circle operations");
    assert!(operations.iter().any(|info| matches!(
        &info.state,
        CircleOperationState::Blocked {
            block: crate::protocol::circle::CircleOperationBlock::AuthorityLost { grant_id }
        } if *grant_id == revoked.author_grant_id
    )));
}

#[tokio::test]
async fn retry_of_a_blocked_operation_republishes_its_exact_prepared_commit() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        "recovery-retry-republish",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let journal = store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare authorized founder operation");
    let operation_id = journal.operation_id.clone();
    let circle_id = journal.circle_id();
    let expected_control = journal.operation().creation.control.coord.clone();
    let expected_commit_object = journal.operation().commit_ref.object.clone();
    let founder_pubkey = keys::public_key_hex(&founder);
    let exact_membership = store
        .bind_device(&db, &founder)
        .await
        .expect("bind founder Store")
        .membership_for_test()
        .await
        .expect("load the founder's exact Store grant");
    let [author_grant_id] = exact_membership
        .active_grant_ids(&founder_pubkey)
        .into_iter()
        .collect::<Vec<_>>()
        .try_into()
        .expect("founder has one active Store grant");
    crate::database::StoreDatabase::new(&db)
        .insert_circle_operation(journal)
        .await
        .expect("persist authorized operation");

    // The operation is durably blocked (its exact retained payload preserved),
    // then retried. Retry restores the phase and re-enters the publish pipeline
    // without regenerating anything.
    crate::database::StoreDatabase::new(&db)
        .block_circle_operation(
            &operation_id,
            crate::protocol::circle::CircleOperationBlock::AuthorityLost {
                grant_id: author_grant_id,
            },
        )
        .await
        .expect("block the authorized operation");
    store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&operation_id)
        .await
        .expect("retry publishes the still-authorized operation");

    assert!(crate::database::StoreDatabase::new(&db)
        .circle_operation(&operation_id)
        .await
        .expect("read retried operation")
        .is_none());
    let (activated, activation_commit_ref) = crate::database::StoreDatabase::new(&db)
        .circle_authoring_context(circle_id, &keys::public_key_hex(&founder))
        .await
        .expect("load activated Circle authoring state");
    assert_eq!(
        activated.control.coord, expected_control,
        "retry activates the exact prepared control, nothing regenerated"
    );
    assert_eq!(
        activation_commit_ref.object, expected_commit_object,
        "retry publishes the exact prepared commit object"
    );
}

/// Discard refuses an operation with no verified nonactivation proof: an
/// unpublished founder operation whose author is still authorized and whose
/// successor slot is empty. It never assumes the unseen candidate failed to
/// activate — the journal row stays durable.
#[tokio::test]
async fn discard_without_nonactivation_proof_is_refused() {
    let db = open_test_db();
    let (store, _home, signer, journal) =
        persist_merge_operation(&db, "recovery-discard-refusal").await;
    let operation_id = journal.operation_id.clone();

    let refusal = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle discard Store")
        .circles()
        .discard_circle_operation(&operation_id)
        .await
        .expect_err("discard without a nonactivation proof is refused");
    assert!(
        matches!(
            &refusal,
            CircleOperationError::DiscardRequiresNonactivation { operation_id: refused }
                if *refused == operation_id
        ),
        "{refusal:?}"
    );
    assert!(
        crate::database::StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read operation after refused discard")
            .is_some(),
        "a refused discard leaves the operation durable"
    );
}

/// An accepted Store commit names the membership transition that revokes the
/// exact grant which signed the operation, and its predecessor cut excludes the
/// operation's candidate. After the removed author pulls that public witness,
/// discard verifies it, retires the candidate graph, and clears the journal.
#[tokio::test]
async fn discard_after_membership_revocation_witness_cleans_the_operation() {
    let revoked = RevokedOperation::prepare("recovery-discard-revocation").await;
    revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation");

    let changeset = revoked
        .owner_db
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('circle-revocation-witness', 'Circle revocation witness', NULL, \
                     '0000000001004-0000-founder', '2026-01-01')",
        ])
        .await;
    StoreDatabase::new(&revoked.owner_db)
        .enqueue_store_changeset_for_test(changeset)
        .await
        .expect("enqueue the membership-revocation witness");
    let owner_store = revoked
        .store
        .bind_device(&revoked.owner_db, &revoked.founder)
        .await
        .expect("load the revocation witness Store");
    let mut writer = owner_store
        .authorize_writer()
        .await
        .expect("authorize the revocation witness writer");
    assert!(
        writer
            .prepare_pending_store_write()
            .await
            .expect("prepare the membership-revocation witness"),
        "membership revocation must be named by a Store commit"
    );
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish the membership-revocation witness"),
        1,
        "one accepted Store commit must witness the membership revocation"
    );

    let member_store = revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("load removed member Store");
    let pull = member_store
        .authorize_writer()
        .await
        .expect("authorize removed member Store pull")
        .pull(None)
        .await
        .expect("pull the accepted membership-revocation witness");
    assert!(
        pull.held_positions.is_empty(),
        "membership-revocation witness must materialize: {:?}",
        pull.held_positions
    );

    revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("bind revoked Circle discard Store")
        .circles()
        .discard_circle_operation(&revoked.operation_id)
        .await
        .expect("the accepted membership revocation permits discard");

    assert!(
        StoreDatabase::new(&revoked.db)
            .circle_operation(&revoked.operation_id)
            .await
            .expect("read discarded operation")
            .is_none(),
        "discard clears the revoked author's journal row"
    );
}

/// A different verified winner claims the operation's device-stream successor
/// slot. Discard proves the Merge winner, exact-deletes the loser's
/// candidate-exclusive objects with absence verified, leaves the winner's
/// published objects untouched, and clears the journal row.
#[tokio::test]
async fn discard_after_slot_lost_to_verified_winner_cleans_candidate_exclusive_objects() {
    let db = open_test_db();
    let (store, _home, signer, journal) =
        persist_merge_operation(&db, "recovery-discard-winner").await;
    let operation_id = journal.operation_id.clone();
    let candidate_commit = journal.operation().commit_ref.object.clone();

    let (winner_commit, winner_head) = store.publish_competing_store_head(&journal).await;

    // Publishing the operation uploads its candidate graph, then loses the head
    // slot to the winner already occupying it.
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .publish_circle_operation(&operation_id)
        .await
        .expect_err("publication loses the successor slot to the winner");
    assert!(
        _home.contains_exact_object(&candidate_commit),
        "the candidate commit reached cloud storage before the slot was lost"
    );

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle discard Store")
        .circles()
        .discard_circle_operation(&operation_id)
        .await
        .expect("the verified winner permits discard");

    assert!(
        crate::database::StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read discarded operation")
            .is_none(),
        "discard clears the journal row"
    );
    assert!(!_home.contains_exact_object(&candidate_commit));
    assert!(
        !db.remote_object_exists_for_test(candidate_commit.clone())
            .await
            .expect("check stored remote object"),
        "the candidate commit's remote-object row is deleted"
    );
    assert!(
        _home.contains_exact_object(&winner_commit),
        "the winner's commit is untouched"
    );
    assert!(
        _home.contains_exact_object(&winner_head),
        "the winner's activation head is untouched"
    );
}

/// A crash during cleanup — the first exact deletion fails after the proof and
/// `Discarding` state are already durable — leaves the operation resumable.
/// Resume re-runs the idempotent cleanup and clears the journal exactly once.
#[tokio::test]
async fn discard_resumes_after_a_crash_at_the_cleanup_boundary() {
    let db = open_test_db();
    let (store, _home, signer, journal) =
        persist_merge_operation(&db, "recovery-discard-crash").await;
    let operation_id = journal.operation_id.clone();
    let candidate_commit = journal.operation().commit_ref.object.clone();

    store.publish_competing_store_head(&journal).await;
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .publish_circle_operation(&operation_id)
        .await
        .expect_err("publication loses the successor slot to the winner");

    // Fail the first candidate-exclusive deletion, after the transaction that
    // recorded the proof and moved the row into `Discarding` has committed.
    _home.fail_exact_delete_on_call(1);
    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle discard Store")
        .circles()
        .discard_circle_operation(&operation_id)
        .await
        .expect_err("the injected delete failure interrupts cleanup");
    assert_eq!(
        crate::database::StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read interrupted operation")
            .expect("interrupted discard stays durable")
            .state(),
        CircleOperationState::Discarding,
        "the interrupted discard is durably resumable"
    );

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume completes the interrupted discard");
    assert!(
        crate::database::StoreDatabase::new(&db)
            .circle_operation(&operation_id)
            .await
            .expect("read resumed operation")
            .is_none(),
        "resume clears the discarded operation's journal row"
    );
    assert!(!_home.contains_exact_object(&candidate_commit));
}

#[tokio::test]
async fn retry_refuses_active_operations_and_reblocks_idempotently() {
    let db = open_test_db();
    let founder = UserKeypair::generate();
    let store = create_test_store_in_its_own_task(
        &db,
        "recovery-retry-refusal",
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let ready = store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000001000-0000-founder", "Household")
        .await
        .expect("prepare an authorized operation");
    let ready_id = ready.operation_id.clone();
    crate::database::StoreDatabase::new(&db)
        .insert_circle_operation(ready)
        .await
        .expect("persist the ready operation");
    let refusal = store
        .bind_device(&db, &founder)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&ready_id)
        .await
        .expect_err("retrying an operation that is not blocked is refused");
    assert!(
        matches!(&refusal, CircleOperationError::NotBlocked { operation_id } if *operation_id == ready_id),
        "{refusal}"
    );

    // A permanently-blocked operation re-blocks on retry; retrying twice leaves it
    // durably blocked with no corruption.
    let revoked = RevokedOperation::prepare("recovery-retry-reblock").await;
    revoked
        .store
        .bind_device(&revoked.db, &revoked.successor)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("resume blocks the revoked operation");
    for _ in 0..2 {
        match revoked
            .store
            .bind_device(&revoked.db, &revoked.successor)
            .await
            .expect("bind Circle test Store")
            .retry_circle_operation(&revoked.operation_id)
            .await
        {
            Err(CircleOperationError::Blocked { .. }) => {}
            other => panic!("retry of a permanently-blocked operation must re-block: {other:?}"),
        }
        assert!(matches!(
            StoreDatabase::new(&revoked.db)
                .circle_operation(&revoked.operation_id)
                .await
                .expect("read re-blocked operation")
                .expect("operation remains durable")
                .state(),
            CircleOperationState::Blocked { .. }
        ));
    }
}

/// A writer takes the operation's stream position between the composition that
/// claimed it and the publication that uses it. The candidate commit is bound to
/// that create-once head slot, so no republish can ever take it: the operation
/// blocks, typed and visible, and — the point — the resume queue advances past it
/// instead of retrying the loser forever and stranding every operation behind it.
#[tokio::test]
async fn a_lost_position_blocks_its_operation_and_releases_the_queue_behind_it() {
    let db = open_test_db();
    let (store, _home, signer, first) =
        persist_merge_operation(&db, "recovery-position-lost").await;
    let second = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle preparation Store")
        .prepare_circle_operation("0000000002000-0000-creator", "Second household")
        .await
        .expect("prepare the operation queued behind the loser");
    let second_id = second.operation_id.clone();
    crate::database::StoreDatabase::new(&db)
        .insert_circle_operation(second.clone())
        .await
        .expect("persist the operation queued behind the loser");

    store.publish_competing_store_head(&first).await;

    store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .resume_circle_operations()
        .await
        .expect("the resume queue drains past an operation that lost its position");

    let second_after = crate::database::StoreDatabase::new(&db)
        .circle_operation(&second_id)
        .await
        .expect("read the operation queued behind the loser")
        .expect("the operation queued behind the loser stays durable");
    assert!(
        matches!(
            second_after.state(),
            CircleOperationState::Blocked {
                block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
            }
        ),
        "the queue advanced to and classified the next operation: {:?}",
        second_after.state(),
    );
    let blocked = crate::database::StoreDatabase::new(&db)
        .circle_operation(&first.operation_id)
        .await
        .expect("read the operation that lost its position")
        .expect("a blocked operation stays durable");
    assert!(
        matches!(
            blocked.state(),
            CircleOperationState::Blocked {
                block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
            }
        ),
        "the lost position blocks the operation: {:?}",
        blocked.state(),
    );

    // The block is a fact reported to the initiator, so it has to be legible from
    // the surface the initiator reads.
    let reported = crate::database::StoreDatabase::new(&db)
        .get_circle_operations()
        .await
        .expect("list circle operations");
    let loser = reported
        .iter()
        .find(|info| info.operation_id == first.operation_id)
        .expect("the loser is still listed");
    assert!(
        matches!(
            &loser.state,
            CircleOperationState::Blocked {
                block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
            }
        ),
        "the initiator can see why the operation stopped: {:?}",
        loser.state,
    );

    assert!(
        crate::database::StoreDatabase::new(&db)
            .oldest_pending_circle_operation()
            .await
            .expect("read the publish queue head")
            .is_none_or(|pending| pending.operation_id != first.operation_id),
        "the blocked loser is no longer the head of the publish queue",
    );

    // Retrying a permanently lost position must not re-wedge the queue: it
    // unblocks, re-observes the same winner, and re-blocks typed.
    let retried = store
        .bind_device(&db, &signer)
        .await
        .expect("bind Circle test Store")
        .retry_circle_operation(&first.operation_id)
        .await
        .expect_err("a position that is gone cannot be retried into");
    assert!(
        matches!(
            &retried,
            CircleOperationError::Blocked {
                block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
                ..
            }
        ),
        "retry re-blocks typed rather than looping: {retried}",
    );
    assert!(
        matches!(
            crate::database::StoreDatabase::new(&db)
                .circle_operation(&first.operation_id)
                .await
                .expect("read the retried operation")
                .expect("the retried operation stays durable")
                .state(),
            CircleOperationState::Blocked {
                block: crate::protocol::circle::CircleOperationBlock::PositionLost { .. },
            }
        ),
        "the retried operation is left blocked, not pending",
    );
}
