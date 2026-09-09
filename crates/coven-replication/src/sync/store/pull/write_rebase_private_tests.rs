use super::RebaseFixture;
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_protocol::write::{WriteBlock, WriteRebaseConflictReason, WriteStatus};

#[tokio::test]
async fn snapshot_rebase_preserves_rows_made_private_after_their_accepted_sharing() {
    let fixture = RebaseFixture::new().await;
    RebaseFixture::capture_host_edit(&fixture.source, &fixture.routing,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('returned-private', 'Private original', 0, '0000000002000-0000-owner', '2026-01-01');
         INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) VALUES
         ('returned-tag', 'returned-private', 'Private child', '0000000002000-0000-owner', '2026-01-01')",
    ).await;
    RebaseFixture::capture_host_edit(
        &fixture.source,
        &fixture.routing,
        "UPDATE notes SET shared = 1, _updated_at = '0000000002100-0000-owner'
         WHERE id = 'returned-private'",
    )
    .await;
    RebaseFixture::publish(&fixture.owner).await;
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer accepts sharing");
    assert!(
        fixture
            .target
            .test_row_exists("SELECT 1 FROM note_tags WHERE id = 'returned-tag'")
            .await
    );
    RebaseFixture::capture_host_edit(&fixture.source, &fixture.routing,
        "UPDATE notes SET shared = 0, title = 'Private again', _updated_at = '0000000002200-0000-owner'
         WHERE id = 'returned-private'",
    ).await;
    RebaseFixture::publish(&fixture.owner).await;
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer accepts removal from sharing");
    assert!(
        !fixture
            .target
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'returned-private'")
            .await
    );
    RebaseFixture::capture_host_edit(
        &fixture.source,
        &fixture.routing,
        "UPDATE note_tags SET tag = 'Later private child', _updated_at = '0000000002300-0000-owner'
         WHERE id = 'returned-tag'",
    )
    .await;
    fixture.snapshot_peer_edit(false).await;
    fixture
        .owner
        .pull_store()
        .await
        .expect("adopt checkpoint preserving later private effects");
    assert_eq!(
        fixture
            .source
            .query_test_text(
                "SELECT title || ':' || shared FROM notes WHERE id = 'returned-private'",
            )
            .await,
        "Private again:0"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT tag FROM note_tags WHERE id = 'returned-tag'",)
            .await,
        "Later private child"
    );
    assert!(
        !fixture
            .target
            .test_row_exists("SELECT 1 FROM note_tags WHERE id = 'returned-tag'")
            .await
    );
}

#[tokio::test]
async fn snapshot_rebase_rejects_an_independent_folded_private_row_collision() {
    let fixture = RebaseFixture::new().await;
    RebaseFixture::capture_host_edit(&fixture.source, &fixture.routing,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('folded-conflict', 'Independent private row', 0, '0000000002000-0000-owner', '2026-01-01')",
    ).await;
    let database = StoreDatabase::new(&fixture.source);
    let before = database
        .store_current_publication()
        .await
        .expect("original boundary");
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("original baseline");
    RebaseFixture::capture_host_edit(
        &fixture.target,
        &fixture.routing,
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('folded-conflict', 'Independent shared row', 1, '0000000002100-0000-peer', '2026-01-01')",
    )
    .await;
    RebaseFixture::publish(&fixture.peer).await;
    fixture
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .expect("peer checkpoint");
    let error = fixture
        .owner
        .pull_store()
        .await
        .expect_err("independent private row must conflict");
    assert!(
        format!("{error:?}")
            .contains("accepted checkpoint conflicts with folded Local row notes/folded-conflict"),
        "{error:?}"
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("preserved boundary"),
        before
    );
    let after = database
        .installed_replay_baseline()
        .await
        .expect("preserved baseline");
    assert_eq!(after.coverage(), baseline.coverage());
    assert_eq!(
        after.snapshot().map(|snapshot| &snapshot.reference),
        baseline.snapshot().map(|snapshot| &snapshot.reference)
    );
    assert_eq!(
        fixture
            .source
            .query_test_text(
                "SELECT title || ':' || shared FROM notes WHERE id = 'folded-conflict'"
            )
            .await,
        "Independent private row:0"
    );
}

#[tokio::test]
async fn snapshot_rebase_blocks_a_conflicting_private_only_suffix_without_losing_private_work() {
    let fixture = RebaseFixture::new().await;
    let prepared = fixture.prepare_local().await;
    let (first_id, _, _) = prepared
        .commit_reservation()
        .expect("shared write reservation");
    let database = StoreDatabase::new(&fixture.source);
    let private = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                     ('private-conflict', 'Private recorded title', 0, \
                      '0000000002500-0000-owner', '2026-01-01')",
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture private-only write through the host owner");
    assert_eq!(private.status, WriteStatus::LocalOnly);
    assert_eq!(
        database
            .write_status(&private.write_id)
            .await
            .expect("private receipt"),
        WriteStatus::LocalOnly,
    );
    let dependent = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch(
                    "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                     SELECT 'private-dependent', title || ' copied', 0, \
                     '0000000002600-0000-owner', created_at FROM notes \
                     WHERE id = 'private-conflict'",
                )?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture later private write that reads the conflicting row");
    assert_eq!(dependent.status, WriteStatus::LocalOnly);
    let boundary = database
        .store_current_publication()
        .await
        .expect("original boundary");
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("original baseline");
    fixture
        .target
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('private-conflict', 'Accepted peer title', 1, \
          '0000000003000-0000-peer', '2026-01-01')",
        )
        .await;
    RebaseFixture::publish(&fixture.peer).await;
    fixture
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .expect("snapshot accepts peer row");

    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume shared publication");
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("private-only intent cannot overwrite the snapshot's accepted shared row");
    drop(writer);
    let Some((faulted_write, WriteBlock::RebaseConflict(conflict))) = error.write_block(first_id)
    else {
        panic!("private conflict lost its typed identity: {error:?}");
    };
    assert_eq!(faulted_write, private.write_id);
    assert_eq!(conflict.write_id, private.write_id);
    assert_eq!(conflict.reason, WriteRebaseConflictReason::PrivateShared,);
    assert_eq!(
        conflict.affected_rows,
        vec![coven_protocol::write::AffectedRow {
            table: "notes".to_string(),
            primary_key: "private-conflict".to_string(),
        }],
    );
    assert_eq!(
        database
            .write_status(first_id)
            .await
            .expect("shared write status"),
        WriteStatus::Publishing,
    );
    assert_eq!(
        database
            .write_status(&private.write_id)
            .await
            .expect("private conflict status"),
        WriteStatus::LocalOnlyBlocked(WriteBlock::RebaseConflict(conflict.clone())),
        "the private-only write must remain available as a blocked receipt: {error:?}",
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved shared reservation"),
        Some(prepared.clone()),
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged boundary"),
        boundary
    );
    let unchanged = database
        .installed_replay_baseline()
        .await
        .expect("unchanged baseline");
    assert_eq!(unchanged.coverage(), baseline.coverage());
    assert_eq!(
        unchanged.snapshot().map(|snapshot| &snapshot.reference),
        baseline.snapshot().map(|snapshot| &snapshot.reference),
    );
    assert_eq!(
        fixture
            .source
            .query_test_text(
                "SELECT title || ':' || shared FROM notes WHERE id = 'private-conflict'",
            )
            .await,
        "Private recorded title:0"
    );
    assert_eq!(
        fixture
            .target
            .query_test_text(
                "SELECT title || ':' || shared FROM notes WHERE id = 'private-conflict'",
            )
            .await,
        "Accepted peer title:1"
    );
    assert_eq!(
        database
            .write_status(&dependent.write_id)
            .await
            .expect("dependent receipt"),
        WriteStatus::LocalOnly,
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM notes WHERE id = 'private-dependent'")
            .await,
        "Private recorded title copied",
    );
    assert!(database
        .blocked_writes()
        .await
        .expect("blocked writes for host")
        .iter()
        .any(|write| write.write_id == private.write_id));
    assert_eq!(
        database
            .retry_blocked_write(&private.write_id)
            .await
            .expect("retry private intent"),
        vec![private.write_id.clone()],
    );
    assert_eq!(
        database
            .write_status(&private.write_id)
            .await
            .expect("retried private status"),
        WriteStatus::LocalOnly,
    );
    assert!(!database
        .pending_writes()
        .await
        .expect("publication queue after private retry")
        .iter()
        .any(|write| write.write_id == private.write_id));
    let retry_error = fixture
        .owner
        .drain_store_writes()
        .await
        .expect_err("retry must preserve the same conflicting private intent");
    assert_eq!(
        retry_error.write_block(first_id),
        Some((
            private.write_id.clone(),
            WriteBlock::RebaseConflict(conflict.clone())
        ))
    );
    assert_eq!(
        database
            .write_status(&private.write_id)
            .await
            .expect("blocked again"),
        WriteStatus::LocalOnlyBlocked(WriteBlock::RebaseConflict(conflict)),
    );
    assert_eq!(
        fixture
            .owner
            .discard_blocked_write(private.write_id.clone())
            .await
            .expect("explicitly resolve the conflicting private suffix"),
        vec![private.write_id.clone(), dependent.write_id.clone()],
    );
    for write_id in [&private.write_id, &dependent.write_id] {
        assert_eq!(
            database
                .write_status(write_id)
                .await
                .expect("resolved private receipt"),
            WriteStatus::Resolved(coven_protocol::write::WriteResolution::Discarded)
        );
    }
    assert!(
        !fixture
            .source
            .test_row_exists(
                "SELECT 1 FROM notes WHERE id = 'private-conflict' OR id = 'private-dependent'"
            )
            .await
    );
    assert_eq!(
        fixture
            .owner
            .drain_store_writes()
            .await
            .expect("publish retained shared work after explicit private resolution"),
        1
    );
    assert_eq!(
        fixture
            .source
            .query_test_text(
                "SELECT title || ':' || shared FROM notes WHERE id = 'private-conflict'"
            )
            .await,
        "Accepted peer title:1"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM notes WHERE id = 'private'")
            .await,
        "Private local effect"
    );
}
