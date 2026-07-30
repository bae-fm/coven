use super::*;

#[tokio::test]
async fn accepted_package_transfers_to_shared_live_set_ownership() {
    let fixture = prepared_write_fixture().await;

    assert!(
        remote_object_exists(&fixture.db, &fixture.head_object).await,
        "the prepared Merge head must have durable candidate ownership before publication",
    );

    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("publish prepared Store package"),
        1,
    );

    let stream_id = commit_stream(&fixture.commit_ref);
    let retained_input: Vec<u8> = fixture
        .db
        .call(move |conn| {
            conn.query_row(
                "SELECT canonical_input FROM retained_merge_materializations
                 WHERE device_id = ?1 AND seq = 1",
                [stream_id],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)
        })
        .await
        .expect("load retained local package application");
    let retained_input: serde_json::Value =
        serde_json::from_slice(&retained_input).expect("parse retained local package application");
    assert_eq!(
        retained_input["activation"]["package_application"],
        serde_json::Value::String("locally_authored".to_string()),
    );

    let remote = stored_remote_object(&fixture.db, &fixture.package_object).await;
    assert!(matches!(
        remote,
        crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                record.identity.domain,
                crate::protocol::remote_object::SharedLiveSetObjectDomain::StorePackage { .. }
            )
                && matches!(
                    &record.state,
                    crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.is_empty()
                        && ownership.activated.contains(
                            &crate::protocol::remote_object::SharedObjectOwner::StoreCommit(
                                fixture.commit_ref.clone()
                            )
                        )
                        && ownership.activated.iter().any(|owner| matches!(
                            owner,
                            crate::protocol::remote_object::SharedObjectOwner::RetainedReplay(
                                crate::protocol::remote_object::RetainedReplayOwner::Commit {
                                    commit,
                                    ..
                                }
                            ) if commit == &fixture.commit_ref
                        ))
                        && ownership.activated.len() == 2
                )
    ));
    let commit = stored_remote_object(&fixture.db, &fixture.commit_ref.object).await;
    assert!(matches!(
        commit,
        crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                crate::protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                    reference
                } if reference == &fixture.commit_ref
            ) && matches!(
                &record.state,
                crate::protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                    ownership
                } if ownership.pending.is_empty()
                    && ownership.activated
                        == std::collections::BTreeSet::from([fixture.commit_ref.clone()])
            )
    ));
    let head = stored_remote_object(&fixture.db, &fixture.head_object).await;
    assert!(matches!(
        head,
        crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                crate::protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                    reference
                } if reference.object == fixture.head_object
            ) && matches!(
                &record.state,
                crate::protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                    ownership
                } if ownership.pending.is_empty()
                    && ownership.activated
                        == std::collections::BTreeSet::from([fixture.commit_ref.clone()])
            )
    ));
}

#[tokio::test]
async fn failures_before_package_commit_and_head_keep_the_exact_prepared_write_retryable() {
    for failed_call in 1..=3 {
        let fixture = prepared_write_fixture().await;
        fixture.home.fail_exact_create_before_call(failed_call);
        let first = drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair).await;
        assert!(first.is_err(), "exact create call {failed_call} fails");
        assert_eq!(
            fixture
                .database
                .write_status(&fixture.write_id)
                .await
                .unwrap(),
            crate::WriteStatus::Publishing,
            "transport failure retains the exact prepared write for retry",
        );
        assert!(
            fixture
                .database
                .clone()
                .oldest_prepared_store_write()
                .await
                .unwrap()
                .is_some(),
            "the exact prepared write remains after exact create call {failed_call}",
        );
        assert_eq!(
            fixture
                .database
                .clone()
                .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                .await
                .unwrap(),
            None,
            "local position cannot advance before a verified head",
        );
        assert_eq!(
            exact_object_exists(&fixture.home, &fixture.package_object),
            failed_call > 1,
        );
        assert_eq!(
            exact_object_exists(&fixture.home, &fixture.commit_ref.object),
            failed_call > 2,
        );
        assert!(!exact_object_exists(&fixture.home, &fixture.head_object),);

        assert_eq!(
            drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
                .await
                .expect("retry exact outbound batch"),
            1,
        );
        assert!(fixture
            .database
            .clone()
            .oldest_prepared_store_write()
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            fixture
                .database
                .clone()
                .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
                .await
                .unwrap(),
            Some(fixture.commit_ref.clone()),
        );
        assert!(matches!(
            fixture.database.write_status(&fixture.write_id).await.unwrap(),
            crate::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    crate::PublishedPosition { device_id, commit }
                        if device_id == &fixture.device_id
                            && commit.coord.sequence() == 1
                            && commit.commit_hash == fixture.commit_ref.commit_hash
                )
        ));
    }
}

#[tokio::test]
async fn competing_merge_head_blocks_the_candidate_with_durable_winner_evidence() {
    let fixture = prepared_write_fixture().await;
    let winner = publish_competing_merge_head(&fixture).await;

    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("classify the occupied Merge successor slot"),
        0,
    );
    assert!(matches!(
        fixture.database.write_status(&fixture.write_id).await.unwrap(),
        crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { ref reason })
            if reason.contains(&winner.head_hash.to_string())
    ));
    let write_id = fixture.write_id.clone();
    let retains_prepared = fixture
        .db
        .call(move |conn| {
            conn.query_row(
                "SELECT prepared IS NOT NULL FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(crate::DbError::from)
        })
        .await
        .expect("check durable losing candidate");
    assert!(retains_prepared);
    let package = stored_remote_object(&fixture.db, &fixture.package_object).await;
    assert!(matches!(
        package,
        crate::protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record)
            if matches!(
                &record.state,
                crate::protocol::remote_object::CandidateObjectState::CleanupPending {
                    former_candidates
                } if former_candidates.len() == 1
                    && matches!(
                        former_candidates[0].proof(),
                        crate::protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                            winner_head
                        } if winner_head == &winner
                    )
            )
    ));
    let commit = stored_remote_object(&fixture.db, &fixture.commit_ref.object).await;
    assert!(matches!(
        commit,
        crate::protocol::remote_object::RemoteObjectRecord::CandidateCommit(record)
            if matches!(
                &record.state,
                crate::protocol::remote_object::CandidateCommitState::CleanupPending {
                    proof: crate::protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                        winner_head
                    }
                } if winner_head == &winner
            )
    ));
    let head = stored_remote_object(&fixture.db, &fixture.head_object).await;
    assert!(matches!(
        head,
        crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.state,
                crate::protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
                    former_candidates
                } if former_candidates.len() == 1
                    && matches!(
                        former_candidates[0].proof(),
                        crate::protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                            winner_head
                        } if winner_head == &winner
                    )
            )
    ));
    assert_eq!(
        fixture
            .database
            .clone()
            .discard_blocked_write(&fixture.write_id)
            .await
            .expect("inspect blocked Merge discard"),
        crate::database::BlockedWriteDiscard::RemoteResolutionRequired,
    );
    assert!(fixture
        .database
        .clone()
        .merge_candidate_cleanup_pending(&fixture.write_id)
        .await
        .expect("read pending Merge cleanup"));
    let retry_error = fixture
        .database
        .clone()
        .retry_blocked_write(&fixture.write_id)
        .await
        .expect_err("an occupied immutable Merge slot cannot be retried");
    assert!(
        retry_error.to_string().contains("winner"),
        "unexpected retry error: {retry_error}"
    );
    fixture.home.fail_exact_delete_on_call(2);
    assert!(fixture
        .store
        .cleanup_merge_candidate_for_test(fixture.write_id.clone())
        .await
        .is_err());
    assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    fixture
        .store
        .cleanup_merge_candidate_for_test(fixture.write_id.clone())
        .await
        .expect("resume exact losing Merge cleanup");
    assert!(!fixture
        .database
        .clone()
        .merge_candidate_cleanup_pending(&fixture.write_id)
        .await
        .expect("read completed Merge cleanup"));
    assert_eq!(
        fixture
            .database
            .clone()
            .discard_blocked_write(&fixture.write_id)
            .await
            .expect("discard the cleaned losing Merge write"),
        crate::database::BlockedWriteDiscard::Discarded(vec![fixture.write_id.clone()]),
    );
    assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(!exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(exact_object_exists(&fixture.home, &winner.object));
}

#[tokio::test]
async fn blocked_merge_candidate_is_abandoned_before_local_discard() {
    let fixture = prepared_write_fixture().await;
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block prepared Merge candidate");

    let store = Store::load(
        fixture.database.clone(),
        fixture.storage.clone(),
        fixture.keypair.clone(),
    )
    .await
    .expect("load Store write recovery owner");
    let discarded = store
        .discard_blocked_write(fixture.write_id.clone())
        .await
        .expect("abandon and discard the blocked candidate");
    assert_eq!(discarded, vec![fixture.write_id.clone()]);
    assert!(!fixture
        .database
        .clone()
        .merge_candidate_cleanup_pending(&fixture.write_id)
        .await
        .expect("candidate cleanup completed"));
    let authority = fixture
        .store
        .latest_local_store_position()
        .await
        .expect("read local Merge position")
        .expect("abandonment advances the local stream");
    assert_ne!(authority, fixture.commit_ref);
    let authority_remote = stored_remote_object(&fixture.db, &authority.object).await;
    assert!(matches!(
        authority_remote,
        crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                crate::protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                    reference
                } if reference == &authority
            )
    ));
    assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(!exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(matches!(
        fixture
            .database
            .write_status(&fixture.write_id)
            .await
            .unwrap(),
        crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
    ));
    assert!(remote_object_exists(&fixture.db, &authority.object).await);
}

#[tokio::test]
async fn prepared_merge_abandonment_resumes_after_restart() {
    let fixture = prepared_write_fixture().await;
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block prepared Merge candidate");
    assert!(prepare_merge_candidate_abandonment(
        &fixture.db,
        &fixture.storage,
        &fixture.keypair,
        fixture.write_id.clone(),
    )
    .await
    .expect("persist Merge abandonment"));

    assert_eq!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("resume Merge abandonment"),
        MergeCandidateAbandonment::Abandoned,
    );
}

#[tokio::test]
async fn merge_abandonment_retries_commit_and_head_publication_failures() {
    for failed_call in 1..=2 {
        let fixture = prepared_write_fixture().await;
        fixture
            .database
            .clone()
            .set_write_status(
                &fixture.write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                }),
            )
            .await
            .expect("block prepared Merge candidate");
        assert!(prepare_merge_candidate_abandonment(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("persist Merge abandonment"));
        fixture.home.fail_exact_create_before_call(failed_call);

        assert!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .is_err(),
            "exact create call {failed_call} fails",
        );
        assert_eq!(
            abandon_merge_candidate(
                &fixture.db,
                &fixture.storage,
                &fixture.keypair,
                fixture.write_id.clone(),
            )
            .await
            .expect("retry Merge abandonment publication"),
            MergeCandidateAbandonment::Abandoned,
        );
    }
}

#[tokio::test]
async fn accepted_merge_abandonment_retries_losing_object_deletion() {
    let fixture = prepared_write_fixture().await;
    let batch = fixture
        .database
        .clone()
        .oldest_prepared_store_write()
        .await
        .expect("load prepared Merge write")
        .expect("prepared Merge write exists");
    publish_prepared_remote_objects(
        &fixture.db,
        &fixture.storage,
        &fixture.write_id,
        fixture.root.store_root_hash,
    )
    .await
    .expect("publish original candidate objects");
    fixture
        .storage
        .create_protocol_object(&batch.commit.prepared)
        .await
        .expect("publish original candidate commit");
    fixture
        .database
        .clone()
        .mark_candidate_commit_uploaded(fixture.commit_ref.clone())
        .await
        .expect("record uploaded original candidate commit");
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block prepared Merge candidate");
    fixture.home.fail_exact_delete_on_call(1);

    assert!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .is_err(),
        "losing object deletion fails",
    );
    assert!(fixture
        .database
        .clone()
        .merge_candidate_cleanup_pending(&fixture.write_id)
        .await
        .expect("cleanup remains pending"));
    assert_eq!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("retry losing object deletion"),
        MergeCandidateAbandonment::Abandoned,
    );
}

#[tokio::test]
async fn original_candidate_activation_wins_abandonment_race() {
    let fixture = prepared_write_fixture().await;
    let batch = fixture
        .database
        .clone()
        .oldest_prepared_store_write()
        .await
        .expect("load prepared Merge write")
        .expect("prepared Merge write exists");
    publish_prepared_remote_objects(
        &fixture.db,
        &fixture.storage,
        &fixture.write_id,
        fixture.root.store_root_hash,
    )
    .await
    .expect("publish original candidate objects");
    fixture
        .storage
        .create_protocol_object(&batch.commit.prepared)
        .await
        .expect("publish original candidate commit");
    fixture
        .storage
        .create_protocol_object(&batch.head.prepared)
        .await
        .expect("activate original candidate");
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block locally unobserved Merge activation");

    assert_eq!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("settle activation that won abandonment race"),
        MergeCandidateAbandonment::CandidateActivated,
    );
    assert!(matches!(
        fixture.database.write_status(&fixture.write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if position.commit() == &fixture.commit_ref
    ));
    assert!(exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(exact_object_exists(&fixture.home, &fixture.head_object));
}

#[tokio::test]
async fn third_candidate_wins_after_abandonment_preparation() {
    let fixture = prepared_write_fixture().await;
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block prepared Merge candidate");
    assert!(prepare_merge_candidate_abandonment(
        &fixture.db,
        &fixture.storage,
        &fixture.keypair,
        fixture.write_id.clone(),
    )
    .await
    .expect("persist Merge abandonment"));
    let authority = fixture
        .database
        .clone()
        .oldest_prepared_store_write()
        .await
        .expect("load prepared abandonment")
        .expect("prepared abandonment exists");
    let authority_commit = authority.commit.object.clone();
    let authority_head = authority.head.object.clone();
    let winner = publish_competing_merge_head(&fixture).await;

    assert_eq!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("settle third-candidate winner"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!fixture
        .database
        .clone()
        .merge_candidate_cleanup_pending(&fixture.write_id)
        .await
        .expect("all losing candidates are cleaned"));
    assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(!exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(exact_object_exists(&fixture.home, &winner.object));
    assert!(!remote_object_exists(&fixture.db, &authority_commit).await);
    assert!(!remote_object_exists(&fixture.db, &authority_head).await);
    assert_eq!(
        fixture
            .database
            .clone()
            .discard_blocked_write(&fixture.write_id)
            .await
            .expect("reverse the abandoned local write"),
        crate::database::BlockedWriteDiscard::Discarded(vec![fixture.write_id.clone()]),
    );
}

#[tokio::test]
async fn alternate_head_for_abandonment_authority_is_accepted() {
    let fixture = prepared_write_fixture().await;
    fixture
        .database
        .clone()
        .set_write_status(
            &fixture.write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            }),
        )
        .await
        .expect("block prepared Merge candidate");
    assert!(prepare_merge_candidate_abandonment(
        &fixture.db,
        &fixture.storage,
        &fixture.keypair,
        fixture.write_id.clone(),
    )
    .await
    .expect("persist Merge abandonment"));
    let accepted_head = publish_alternate_head_for_prepared_commit(&fixture).await;

    assert_eq!(
        abandon_merge_candidate(
            &fixture.db,
            &fixture.storage,
            &fixture.keypair,
            fixture.write_id.clone(),
        )
        .await
        .expect("accept alternate abandonment head"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(!exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(exact_object_exists(&fixture.home, &accepted_head.object));
}

#[tokio::test]
async fn alternate_merge_head_for_the_exact_commit_completes_as_accepted() {
    let fixture = prepared_write_fixture().await;
    let accepted_head = publish_alternate_head_for_prepared_commit(&fixture).await;

    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("accept the exact commit through the occupied Merge head"),
        1,
    );
    assert!(matches!(
        fixture.database.write_status(&fixture.write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if position.commit() == &fixture.commit_ref
    ));
    let head = stored_remote_object(&fixture.db, &accepted_head.object).await;
    assert!(matches!(
        head,
        crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                crate::protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                    reference
                } if reference == &accepted_head
            )
    ));
}

#[tokio::test]
async fn exact_create_readback_mismatch_retains_the_prepared_write_for_retry() {
    let fixture = prepared_write_fixture().await;
    fixture.home.corrupt_exact_readback_on_call(1);

    let result = drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair).await;

    assert!(matches!(
        result,
        Err(StoreError::Object(StoreObjectError::Storage(_)))
    ));
    assert_eq!(
        fixture
            .database
            .write_status(&fixture.write_id)
            .await
            .unwrap(),
        crate::WriteStatus::Publishing,
        "a provider readback mismatch retains the exact prepared write for retry",
    );
    assert!(fixture
        .database
        .clone()
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn lost_exact_head_response_is_settled_by_readback_and_completion_is_idempotent() {
    let fixture = prepared_write_fixture().await;
    fixture.home.fail_exact_create_after_call(3);
    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("settle lost head response by exact readback"),
        1
    );
    assert!(exact_object_exists(&fixture.home, &fixture.package_object));
    assert!(exact_object_exists(
        &fixture.home,
        &fixture.commit_ref.object
    ));
    assert!(exact_object_exists(&fixture.home, &fixture.head_object));
    assert_eq!(
        fixture
            .database
            .clone()
            .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
            .await
            .unwrap(),
        Some(fixture.commit_ref.clone())
    );

    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("already-completed exact batch is idempotent"),
        0
    );
    assert!(matches!(
        fixture.database.write_status(&fixture.write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                crate::PublishedPosition { commit, .. }
                    if commit.coord.sequence() == 1
                        && commit.commit_hash == fixture.commit_ref.commit_hash
            )
    ));
}

#[tokio::test]
async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
    let fixture = prepared_write_fixture().await;
    fixture
        .db
        .call(|conn| {
            conn.execute_batch(
                "CREATE TEMP TRIGGER fail_outbound_completion \
                 BEFORE UPDATE OF prepared ON store_writes \
                 WHEN OLD.prepared IS NOT NULL AND NEW.prepared IS NULL \
                 BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("install completion fault");
    let first = drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair).await;
    assert!(matches!(first, Err(StoreError::Database(_))));
    assert_eq!(
        fixture
            .database
            .write_status(&fixture.write_id)
            .await
            .unwrap(),
        crate::WriteStatus::Publishing,
    );
    assert!(exact_object_exists(&fixture.home, &fixture.head_object));
    assert!(fixture
        .database
        .clone()
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        fixture
            .database
            .clone()
            .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
            .await
            .unwrap(),
        None,
        "position and prepared-state clearing share the failed transaction",
    );

    fixture
        .db
        .call(|conn| {
            conn.execute_batch("DROP TRIGGER fail_outbound_completion")
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("remove completion fault");
    assert_eq!(
        drain_store_writes(&fixture.db, &fixture.storage, &fixture.keypair)
            .await
            .expect("retry local completion"),
        1
    );
    assert_eq!(
        fixture
            .database
            .clone()
            .exact_materialized_ref(&commit_stream(&fixture.commit_ref), 1)
            .await
            .unwrap(),
        Some(fixture.commit_ref.clone()),
    );
    assert!(matches!(
        fixture.database.write_status(&fixture.write_id).await.unwrap(),
        crate::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                crate::PublishedPosition { commit, .. }
                    if commit.coord.sequence() == 1
                        && commit.commit_hash == fixture.commit_ref.commit_hash
            )
    ));
}

#[tokio::test]
async fn restart_fails_loud_when_a_prepared_write_has_no_usable_exact_root() {
    for invalid_root in [
        None,
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
    ] {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("store.sqlite3");
        let open = || {
            Database::open(
                &path,
                crate::sync::test_helpers::test_synced_tables(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "dev-writer".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
            )
            .expect("open test database")
            .0
        };
        let home = InMemoryCloudHome::new();
        let keypair = UserKeypair::generate();
        let storage = Arc::new(
            CloudSyncStorage::new(
                Arc::new(home),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "prepared-root-status",
                keypair.clone(),
            )
            .expect("in-memory home supports immutable copies"),
        );
        let db = open();
        let (_root, _) =
            initialize_exact_store(&db, &storage, "prepared-root-status", &keypair).await;
        host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('root-status', 'outbound', NULL, 1, \
                     '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(
            prepare_merge_store_write(&db, &storage, &keypair, &store_dir,)
                .await
                .expect("prepare write")
        );
        let write_id = crate::database::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
            .await
            .expect("load prepared write")
            .expect("prepared write exists")
            .commit
            .value
            .write_id
            .clone();
        db.call(move |conn| {
            match invalid_root {
                Some(value) => conn.execute(
                    "UPDATE store_protocol_root_authority SET store_root_hash = ?1",
                    [value],
                ),
                None => conn.execute("DELETE FROM store_protocol_root_authority", []),
            }
            .map(|_| ())
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("make root unusable");
        drop(db);

        let reopened = open();
        let reopened_database = crate::database::StoreDatabase::new(&reopened);
        let result = drain_store_writes(&reopened, &storage, &keypair).await;
        match (invalid_root, result) {
            (
                None,
                Err(StoreError::MissingState {
                    key: "store_root_authority",
                }),
            ) => {}
            (Some(_), Err(StoreError::Database(reason))) => {
                assert!(reason.contains("Store root authority hash differs"));
            }
            (_, result) => panic!("unexpected Store root failure: {result:?}"),
        }
        assert!(matches!(
            reopened_database
                .write_status(&write_id)
                .await
                .expect("write status"),
            crate::WriteStatus::Publishing
        ));
    }
}

#[tokio::test]
async fn authorized_writer_retains_its_exact_root_without_reloading_durable_authority() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(
        CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "blocked-retry",
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies"),
    );
    let db = open_test_db();
    let database = crate::database::StoreDatabase::new(&db);
    let (_root, _) = initialize_exact_store(&db, &storage, "blocked-retry", &keypair).await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('blocked-first', 'first', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    let writes = database
        .pending_writes()
        .await
        .expect("load pending writes");
    let write_id = writes[0].write_id.clone();
    let store = crate::sync::store::Store::load(database.clone(), storage.clone(), keypair.clone())
        .await
        .expect("load Store before invalidating its durable root");
    let mut writer = store
        .authorize_writer()
        .await
        .expect("authorize writer before invalidating its durable root");
    remove_exact_store_root(&db).await;
    let (_store_temp, store_dir) = temp_store_dir();

    assert!(writer
        .prepare_pending_store_write(&store_dir)
        .await
        .expect("prepare with the root retained by the writer capability"));
    assert_eq!(
        database.write_status(&write_id).await.unwrap(),
        crate::WriteStatus::Publishing
    );
    assert!(matches!(
        crate::sync::store::Store::load(database, storage.clone(), keypair.clone(),).await,
        Err(StoreError::MissingState { .. })
    ));
}

#[tokio::test]
async fn discarding_a_blocked_write_atomically_reverses_its_unpublished_suffix() {
    let home = InMemoryCloudHome::new();
    let keypair = UserKeypair::generate();
    let storage = Arc::new(
        CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "blocked-discard",
            keypair.clone(),
        )
        .expect("in-memory home supports immutable copies"),
    );
    let db = open_test_db();
    let database = crate::database::StoreDatabase::new(&db);
    initialize_exact_store(&db, &storage, "blocked-discard", &keypair).await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('discard-first', 'first', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('discard-second', 'second', NULL, 1, \
                 '0000000001001-0000-writer', '2026-01-01')",
    )
    .await;
    let writes = database.pending_writes().await.unwrap();
    let first = writes[0].write_id.clone();
    let second = writes[1].write_id.clone();
    let (_store_temp, store_dir) = temp_store_dir();
    database
        .set_write_status(
            &first,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "discard test precondition".to_string(),
            }),
        )
        .await
        .expect("block the first unpublished write");

    assert_eq!(
        database.discard_blocked_write(&first).await.unwrap(),
        crate::database::BlockedWriteDiscard::Discarded(vec![first.clone(), second.clone()])
    );
    let note_count: i64 = db
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
                .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(note_count, 0);
    assert!(database.pending_writes().await.unwrap().is_empty());
    for write_id in [first, second] {
        assert_eq!(
            database.write_status(&write_id).await.unwrap(),
            crate::WriteStatus::Resolved(crate::WriteResolution::Discarded)
        );
    }

    host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('after-discard', 'after', NULL, 1, \
                 '0000000001002-0000-writer', '2026-01-01')",
    )
    .await;
    assert!(
        prepare_merge_store_write(&db, &storage, &keypair, &store_dir,)
            .await
            .expect("prepare write after discarded blocked writes")
    );
    assert_eq!(
        drain_store_writes(&db, &storage, &keypair).await.unwrap(),
        1
    );
}
