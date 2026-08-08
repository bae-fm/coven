use super::*;

#[tokio::test]
async fn accepted_package_transfers_to_shared_live_set_ownership() {
    let fixture = PreparedWriteFixture::prepare().await;

    assert!(
        fixture.remote_object_exists(&fixture.head_object).await,
        "the prepared Merge head must have durable candidate ownership before publication",
    );

    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("publish prepared Store package"),
        1,
    );

    let retained_input = fixture.retained_canonical_input().await;
    let retained_input: serde_json::Value =
        serde_json::from_slice(&retained_input).expect("parse retained local package application");
    assert_eq!(
        retained_input["activation"]["package_application"],
        serde_json::Value::String("locally_authored".to_string()),
    );

    let remote = fixture.stored_remote_object(&fixture.package_object).await;
    assert!(matches!(
        remote,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                record.identity.domain,
                coven_protocol::remote_object::SharedLiveSetObjectDomain::StorePackage { .. }
            )
                && matches!(
                    &record.state,
                    coven_protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership
                    } if ownership.pending.is_empty()
                        && ownership.activated.contains(
                            &coven_protocol::remote_object::SharedObjectOwner::StoreCommit(
                                fixture.commit_ref.clone()
                            )
                        )
                        && ownership.activated.iter().any(|owner| matches!(
                            owner,
                            coven_protocol::remote_object::SharedObjectOwner::RetainedReplay(
                                coven_protocol::remote_object::RetainedReplayOwner::Commit {
                                    commit,
                                    ..
                                }
                            ) if commit == &fixture.commit_ref
                        ))
                        && ownership.activated.len() == 2
                )
    ));
    let commit = fixture
        .stored_remote_object(&fixture.commit_ref.object)
        .await;
    assert!(matches!(
        commit,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                    reference
                } if reference == &fixture.commit_ref
            ) && matches!(
                &record.state,
                coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
                    ownership
                } if ownership.pending.is_empty()
                    && ownership.activated
                        == std::collections::BTreeSet::from([fixture.commit_ref.clone()])
            )
    ));
    let head = fixture.stored_remote_object(&fixture.head_object).await;
    assert!(matches!(
        head,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                    reference,
                    ..
                } if reference.object == fixture.head_object
            ) && matches!(
                &record.state,
                coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
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
        let fixture = PreparedWriteFixture::prepare().await;
        fixture.fail_exact_create_before_call(failed_call);
        let first = fixture.drain_store_writes().await;
        assert!(first.is_err(), "exact create call {failed_call} fails");
        assert_eq!(
            fixture.write_status().await,
            coven_protocol::write::WriteStatus::Publishing,
            "transport failure retains the exact prepared write for retry",
        );
        assert!(
            fixture.prepared_write().await.commit.value.write_id == fixture.write_id,
            "the exact prepared write remains after exact create call {failed_call}",
        );
        assert_eq!(
            fixture.exact_materialized_ref().await,
            None,
            "local position cannot advance before a verified head",
        );
        assert_eq!(
            fixture.contains_exact_object(&fixture.package_object),
            failed_call > 1,
        );
        assert_eq!(
            fixture.contains_exact_object(&fixture.commit_ref.object),
            failed_call > 2,
        );
        assert!(!fixture.contains_exact_object(&fixture.head_object),);

        assert_eq!(
            fixture
                .drain_store_writes()
                .await
                .expect("retry exact outbound batch"),
            1,
        );
        assert!(!fixture.prepared_write_exists().await);
        assert_eq!(
            fixture.exact_materialized_ref().await,
            Some(fixture.commit_ref.clone()),
        );
        assert!(matches!(
            fixture.write_status().await,
            coven_protocol::write::WriteStatus::Published(position)
                if matches!(
                    position.as_ref(),
                    coven_protocol::write::PublishedPosition { device_id, commit }
                        if device_id == &fixture.device_id
                            && commit.coord.sequence() == 1
                            && commit.commit_hash == fixture.commit_ref.commit_hash
                )
        ));
    }
}

#[tokio::test]
async fn competing_merge_head_blocks_the_candidate_with_durable_winner_evidence() {
    let fixture = PreparedWriteFixture::prepare().await;
    let winner = fixture.publish_competing_merge_head().await;

    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("classify the occupied Merge successor slot"),
        0,
    );
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Blocked(coven_protocol::write::WriteBlock::InvalidProtocolState { ref reason })
            if reason.contains(&winner.head_hash.to_string())
    ));
    let retains_prepared = fixture.write_retains_prepared().await;
    assert!(retains_prepared);
    let package = fixture.stored_remote_object(&fixture.package_object).await;
    assert!(matches!(
        package,
        coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::CandidateObjectState::CleanupPending {
                    former_candidates
                } if former_candidates.len() == 1
                    && matches!(
                        former_candidates[0].proof(),
                        coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                            winner_head
                        } if winner_head == &winner
                    )
            )
    ));
    let commit = fixture
        .stored_remote_object(&fixture.commit_ref.object)
        .await;
    assert!(matches!(
        commit,
        coven_protocol::remote_object::RemoteObjectRecord::CandidateCommit(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::CandidateCommitState::CleanupPending {
                    proof: coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                        winner_head
                    }
                } if winner_head == &winner
            )
    ));
    let head = fixture.stored_remote_object(&fixture.head_object).await;
    assert!(matches!(
        head,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.state,
                coven_protocol::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
                    former_candidates
                } if former_candidates.len() == 1
                    && matches!(
                        former_candidates[0].proof(),
                        coven_protocol::remote_object::CandidateNonactivationProof::MergeWinner {
                            winner_head
                        } if winner_head == &winner
                    )
            )
    ));
    assert_eq!(
        fixture.discard_blocked_write().await,
        coven_database::BlockedWriteDiscard::RemoteResolutionRequired,
    );
    assert!(fixture.merge_candidate_cleanup_pending().await);
    let retry_error = fixture
        .retry_blocked_write()
        .await
        .expect_err("an occupied immutable Merge slot cannot be retried");
    assert!(
        retry_error.to_string().contains("winner"),
        "unexpected retry error: {retry_error}"
    );
    fixture.fail_exact_delete_on_call(2);
    assert!(fixture.cleanup_merge_candidate().await.is_err());
    assert!(!fixture.contains_exact_object(&fixture.package_object));
    assert!(fixture.contains_exact_object(&fixture.commit_ref.object));
    fixture
        .cleanup_merge_candidate()
        .await
        .expect("resume exact losing Merge cleanup");
    assert!(!fixture.merge_candidate_cleanup_pending().await);
    assert_eq!(
        fixture.discard_blocked_write().await,
        coven_database::BlockedWriteDiscard::Discarded(vec![fixture.write_id.clone()]),
    );
    assert!(!fixture.contains_exact_object(&fixture.package_object));
    assert!(!fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(fixture.contains_exact_object(&winner.object));
}

#[tokio::test]
async fn blocked_merge_candidate_is_abandoned_before_local_discard() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;

    let discarded = fixture
        .discard_blocked_candidate()
        .await
        .expect("abandon and discard the blocked candidate");
    assert_eq!(discarded, vec![fixture.write_id.clone()]);
    assert!(!fixture.merge_candidate_cleanup_pending().await);
    let authority = fixture
        .latest_local_store_position()
        .await
        .expect("read local Merge position")
        .expect("abandonment advances the local stream");
    assert_ne!(authority, fixture.commit_ref);
    let authority_remote = fixture.stored_remote_object(&authority.object).await;
    assert!(matches!(
        authority_remote,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::Commit {
                    reference
                } if reference == &authority
            )
    ));
    assert!(!fixture.contains_exact_object(&fixture.package_object));
    assert!(!fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Resolved(
            coven_protocol::write::WriteResolution::Discarded
        )
    ));
    assert!(fixture.remote_object_exists(&authority.object).await);
}

#[tokio::test]
async fn prepared_merge_abandonment_resumes_after_restart() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;
    assert!(fixture
        .prepare_merge_candidate_abandonment()
        .await
        .expect("persist Merge abandonment"));

    assert_eq!(
        fixture
            .abandon_merge_candidate()
            .await
            .expect("resume Merge abandonment"),
        MergeCandidateAbandonment::Abandoned,
    );
}

#[tokio::test]
async fn merge_abandonment_retries_commit_and_head_publication_failures() {
    for failed_call in 1..=2 {
        let fixture = PreparedWriteFixture::prepare().await;
        fixture
            .set_write_status(coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "host chose discard".to_string(),
                },
            ))
            .await;
        assert!(fixture
            .prepare_merge_candidate_abandonment()
            .await
            .expect("persist Merge abandonment"));
        fixture.fail_exact_create_before_call(failed_call);

        assert!(
            fixture.abandon_merge_candidate().await.is_err(),
            "exact create call {failed_call} fails",
        );
        assert_eq!(
            fixture
                .abandon_merge_candidate()
                .await
                .expect("retry Merge abandonment publication"),
            MergeCandidateAbandonment::Abandoned,
        );
    }
}

#[tokio::test]
async fn accepted_merge_abandonment_retries_losing_object_deletion() {
    let fixture = PreparedWriteFixture::prepare().await;
    let batch = fixture.prepared_write().await;
    fixture
        .publish_prepared_remote_objects()
        .await
        .expect("publish original candidate objects");
    fixture
        .publish_prepared_object(&batch.commit.prepared)
        .await;
    fixture.mark_candidate_commit_uploaded().await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;
    fixture.fail_exact_delete_on_call(1);

    assert!(
        fixture.abandon_merge_candidate().await.is_err(),
        "losing object deletion fails",
    );
    assert!(fixture.merge_candidate_cleanup_pending().await);
    assert_eq!(
        fixture
            .abandon_merge_candidate()
            .await
            .expect("retry losing object deletion"),
        MergeCandidateAbandonment::Abandoned,
    );
}

#[tokio::test]
async fn original_candidate_activation_wins_abandonment_race() {
    let fixture = PreparedWriteFixture::prepare().await;
    let batch = fixture.prepared_write().await;
    fixture
        .publish_prepared_remote_objects()
        .await
        .expect("publish original candidate objects");
    fixture
        .publish_prepared_object(&batch.commit.prepared)
        .await;
    fixture.publish_prepared_object(&batch.head.prepared).await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;

    assert_eq!(
        fixture
            .abandon_merge_candidate()
            .await
            .expect("settle activation that won abandonment race"),
        MergeCandidateAbandonment::CandidateActivated,
    );
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Published(position)
            if position.commit() == &fixture.commit_ref
    ));
    assert!(fixture.contains_exact_object(&fixture.package_object));
    assert!(fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(fixture.contains_exact_object(&fixture.head_object));
}

#[tokio::test]
async fn third_candidate_wins_after_abandonment_preparation() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;
    assert!(fixture
        .prepare_merge_candidate_abandonment()
        .await
        .expect("persist Merge abandonment"));
    let authority = fixture.prepared_write().await;
    let authority_commit = authority.commit.prepared.reference().clone();
    let authority_head = authority.head.prepared.reference().clone();
    let winner = fixture.publish_competing_merge_head().await;

    assert_eq!(
        fixture
            .abandon_merge_candidate()
            .await
            .expect("settle third-candidate winner"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!fixture.merge_candidate_cleanup_pending().await);
    assert!(!fixture.contains_exact_object(&fixture.package_object));
    assert!(!fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(fixture.contains_exact_object(&winner.object));
    assert!(!fixture.remote_object_exists(&authority_commit).await);
    assert!(!fixture.remote_object_exists(&authority_head).await);
    assert_eq!(
        fixture.discard_blocked_write().await,
        coven_database::BlockedWriteDiscard::Discarded(vec![fixture.write_id.clone()]),
    );
}

#[tokio::test]
async fn alternate_head_for_abandonment_authority_is_accepted() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture
        .set_write_status(coven_protocol::write::WriteStatus::Blocked(
            coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: "host chose discard".to_string(),
            },
        ))
        .await;
    assert!(fixture
        .prepare_merge_candidate_abandonment()
        .await
        .expect("persist Merge abandonment"));
    let accepted_head = fixture.publish_alternate_head_for_prepared_commit().await;

    assert_eq!(
        fixture
            .abandon_merge_candidate()
            .await
            .expect("accept alternate abandonment head"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!fixture.contains_exact_object(&fixture.package_object));
    assert!(!fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(fixture.contains_exact_object(&accepted_head.object));
}

#[tokio::test]
async fn alternate_merge_head_for_the_exact_commit_completes_as_accepted() {
    let fixture = PreparedWriteFixture::prepare().await;
    let accepted_head = fixture.publish_alternate_head_for_prepared_commit().await;

    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("accept the exact commit through the occupied Merge head"),
        1,
    );
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Published(position)
            if position.commit() == &fixture.commit_ref
    ));
    let head = fixture.stored_remote_object(&accepted_head.object).await;
    assert!(matches!(
        head,
        coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                    reference,
                    ..
                } if reference == &accepted_head
            )
    ));
}

#[tokio::test]
async fn exact_create_readback_mismatch_retains_the_prepared_write_for_retry() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture.corrupt_exact_readback_on_call(1);

    let result = fixture.drain_store_writes().await;

    assert!(matches!(
        result,
        Err(StoreError::Object(StoreObjectError::Storage(_)))
    ));
    assert_eq!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Publishing,
        "a provider readback mismatch retains the exact prepared write for retry",
    );
    assert!(fixture.prepared_write_exists().await);
}

#[tokio::test]
async fn lost_exact_head_response_is_settled_by_readback_and_completion_is_idempotent() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture.fail_exact_create_after_call(3);
    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("settle lost head response by exact readback"),
        1
    );
    assert!(fixture.contains_exact_object(&fixture.package_object));
    assert!(fixture.contains_exact_object(&fixture.commit_ref.object));
    assert!(fixture.contains_exact_object(&fixture.head_object));
    assert_eq!(
        fixture.exact_materialized_ref().await,
        Some(fixture.commit_ref.clone())
    );

    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("already-completed exact batch is idempotent"),
        0
    );
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                coven_protocol::write::PublishedPosition { commit, .. }
                    if commit.coord.sequence() == 1
                        && commit.commit_hash == fixture.commit_ref.commit_hash
            )
    ));
}

#[tokio::test]
async fn local_completion_failure_rolls_back_position_and_retries_after_visible_head() {
    let fixture = PreparedWriteFixture::prepare().await;
    fixture.install_outbound_completion_failure().await;
    let first = fixture.drain_store_writes().await;
    assert!(matches!(first, Err(StoreError::Database(_))));
    assert_eq!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Publishing,
    );
    assert!(fixture.contains_exact_object(&fixture.head_object));
    assert!(fixture.prepared_write_exists().await);
    assert_eq!(
        fixture.exact_materialized_ref().await,
        None,
        "position and prepared-state clearing share the failed transaction",
    );

    fixture.remove_outbound_completion_failure().await;
    assert_eq!(
        fixture
            .drain_store_writes()
            .await
            .expect("retry local completion"),
        1
    );
    assert_eq!(
        fixture.exact_materialized_ref().await,
        Some(fixture.commit_ref.clone()),
    );
    assert!(matches!(
        fixture.write_status().await,
        coven_protocol::write::WriteStatus::Published(position)
            if matches!(
                position.as_ref(),
                coven_protocol::write::PublishedPosition { commit, .. }
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
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                "dev-writer".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
            )
            .expect("open test database")
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
        let device = crate::sync::test_helpers::TestDevice::create(
            &db,
            storage.clone(),
            "prepared-root-status",
            keypair.clone(),
        )
        .await
        .expect("create prepared-root-status Store");
        db.execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('root-status', 'outbound', NULL, 1, \
                     '0000000001000-0000-writer', '2026-01-01')",
        )
        .await;
        let (_store_temp, store_dir) = temp_store_dir();
        assert!(device
            .prepare_pending_store_write(&store_dir)
            .await
            .expect("prepare write"));
        let write_id = coven_database::StoreDatabase::new(&db)
            .oldest_prepared_store_write()
            .await
            .expect("load prepared write")
            .expect("prepared write exists")
            .commit
            .value
            .write_id
            .clone();
        db.test_sql(move |conn| conn.replace_store_root_hash(invalid_root))
            .await
            .expect("make root unusable");
        drop(device);
        drop(db);

        let reopened = open();
        let reopened_database = coven_database::StoreDatabase::new(&reopened);
        let result = match crate::sync::test_helpers::TestDevice::load(
            &reopened,
            storage.clone(),
            keypair.clone(),
        )
        .await
        {
            Ok(device) => device.drain_store_writes().await,
            Err(error) => Err(error),
        };
        match (invalid_root, result) {
            (
                None,
                Err(StoreError::MissingState {
                    key: "store_root_authority",
                }),
            ) => {}
            (Some(_), Err(StoreError::Database(reason))) => {
                assert!(reason
                    .to_string()
                    .contains("Store root authority hash differs"));
            }
            (_, result) => panic!("unexpected Store root failure: {result:?}"),
        }
        assert!(matches!(
            reopened_database
                .write_status(&write_id)
                .await
                .expect("write status"),
            coven_protocol::write::WriteStatus::Publishing
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
    let database = coven_database::StoreDatabase::new(&db);
    let device = crate::sync::test_helpers::TestDevice::create(
        &db,
        storage.clone(),
        "blocked-retry",
        keypair.clone(),
    )
    .await
    .expect("create blocked-retry Store");
    db.execute_test_host_write(
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
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize writer before invalidating its durable root");
    db.remove_store_protocol_root_for_test().await;
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare with the root retained by the writer capability"));
    assert_eq!(
        database.write_status(&write_id).await.unwrap(),
        coven_protocol::write::WriteStatus::Publishing
    );
    drop(writer);
    assert!(matches!(
        crate::sync::test_helpers::TestDevice::load(&db, storage.clone(), keypair.clone()).await,
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
    let database = coven_database::StoreDatabase::new(&db);
    let device = crate::sync::test_helpers::TestDevice::create(
        &db,
        storage.clone(),
        "blocked-discard",
        keypair.clone(),
    )
    .await
    .expect("create blocked-discard Store");
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('discard-first', 'first', NULL, 1, \
                 '0000000001000-0000-writer', '2026-01-01')",
    )
    .await;
    db.execute_test_host_write(
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
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::InvalidProtocolState {
                    reason: "discard test precondition".to_string(),
                },
            ),
        )
        .await
        .expect("block the first unpublished write");

    assert_eq!(
        database.discard_blocked_write(&first).await.unwrap(),
        coven_database::BlockedWriteDiscard::Discarded(vec![first.clone(), second.clone()])
    );
    let note_count: i64 = db
        .test_sql(|conn| {
            conn.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
                .map_err(coven_database::DbError::from)
        })
        .await
        .unwrap();
    assert_eq!(note_count, 0);
    assert!(database.pending_writes().await.unwrap().is_empty());
    for write_id in [first, second] {
        assert_eq!(
            database.write_status(&write_id).await.unwrap(),
            coven_protocol::write::WriteStatus::Resolved(
                coven_protocol::write::WriteResolution::Discarded
            )
        );
    }

    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('after-discard', 'after', NULL, 1, \
                 '0000000001002-0000-writer', '2026-01-01')",
    )
    .await;
    assert!(device
        .prepare_pending_store_write(&store_dir)
        .await
        .expect("prepare write after discarded blocked writes"));
    assert_eq!(device.drain_store_writes().await.unwrap(), 1);
}
