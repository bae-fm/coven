use super::*;
use coven_database::{
    ChangesetColumn, ChangesetOperation, ChangesetRow, ChangesetUpdate, Migration,
    TableChangesetMigration,
};
use coven_protocol::write::{WriteBlock, WriteStatus};

#[tokio::test]
async fn discard_original_effect_after_value_migration_restores_prior_rows() {
    discard_after_value_migration(false, false).await;
}

#[tokio::test]
async fn discard_rebased_effect_after_value_migration_restores_prior_rows() {
    discard_after_value_migration(true, false).await;
}

async fn discard_after_value_migration(rebase: bool, fail_before_retry: bool) {
    let fixture = RebaseFixture::new().await;
    let receipt = StoreDatabase::new(&fixture.source)
        .run_host_store_write_for_test(Some(fixture.routing.clone()), None, |tx| {
            tx.execute_batch("UPDATE notes SET title = 'Local title', _updated_at = '0000000004000-0000-owner' WHERE id = 'shared'")?;
            Ok::<_, coven_database::DbError>(())
        }).await.unwrap();
    let later = StoreDatabase::new(&fixture.source)
        .run_host_store_write_for_test(Some(fixture.routing.clone()), None, |tx| {
            tx.execute_batch("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('local', 'Local row', 0, '0000000005000-0000-owner', '2026-01-01')")?;
            Ok::<_, coven_database::DbError>(())
        }).await.unwrap();
    if rebase {
        fixture.snapshot_peer_edit(false).await;
        fixture
            .owner
            .pull_store()
            .await
            .expect("rebase the unprepared local write");
        assert!(StoreDatabase::new(&fixture.source)
            .has_rebased_store_writes_for_test()
            .await
            .unwrap());
    }
    let mut migrations = test_migrations();
    let reject = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reject_conversion = reject.clone();
    migrations.push(
        Migration::sql(
            2,
            "qualify titles",
            "UPDATE notes SET title = 'migrated:' || title;",
        )
        .changesets(vec![TableChangesetMigration::new(
            "notes",
            &[],
            move |row, _| {
                if reject_conversion.load(std::sync::atomic::Ordering::SeqCst)
                    && matches!(&row.change, ChangesetOperation::Update(_))
                {
                    return Err(coven_database::DbError::Message(
                        "refuse inverse conversion".into(),
                    ));
                }
                qualify_title(row);
                Ok(())
            },
        )]),
    );
    let upgraded = RebaseFixture::open_schema(
        &fixture.path,
        fixture.source_dir.clone(),
        test_synced_tables(),
        &migrations,
    );
    let database = StoreDatabase::new(&upgraded);
    database
        .set_write_status(
            &receipt.write_id,
            WriteStatus::Blocked(WriteBlock::InvalidProtocolState {
                reason: "discard local edit".into(),
            }),
        )
        .await
        .unwrap();
    if fail_before_retry {
        reject.store(true, std::sync::atomic::Ordering::SeqCst);
        let error = database
            .discard_blocked_write(&receipt.write_id)
            .await
            .expect_err("conversion failure rolls back the already applied later inverse");
        assert!(
            error.to_string().contains("refuse inverse conversion"),
            "{error}"
        );
        assert_eq!(
            upgraded
                .query_test_text("SELECT title FROM notes WHERE id='shared'")
                .await,
            "migrated:Local title"
        );
        assert!(
            upgraded
                .test_row_exists("SELECT 1 FROM notes WHERE id='local'")
                .await
        );
        assert_eq!(
            database.write_status(&later.write_id).await.unwrap(),
            WriteStatus::LocalOnly
        );
        reject.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    assert_eq!(
        database
            .discard_blocked_write(&receipt.write_id)
            .await
            .expect("discard historical effect"),
        coven_database::BlockedWriteDiscard::Discarded(vec![receipt.write_id, later.write_id])
    );
    assert_eq!(
        upgraded
            .query_test_text("SELECT title FROM notes WHERE id='shared'")
            .await,
        "migrated:Original title"
    );
    assert!(
        !upgraded
            .test_row_exists("SELECT 1 FROM notes WHERE id='local'")
            .await
    );
}

#[tokio::test]
async fn rebased_effect_keeps_its_own_schema_across_another_upgrade() {
    discard_effect_rebased_under_new_schema(false).await;
}

#[tokio::test]
async fn legacy_rebased_effect_recovers_its_own_schema_across_another_upgrade() {
    discard_effect_rebased_under_new_schema(true).await;
}

async fn discard_effect_rebased_under_new_schema(legacy: bool) {
    let fixture = RebaseFixture::new().await;
    let receipt = StoreDatabase::new(&fixture.source)
        .run_host_store_write_for_test(Some(fixture.routing.clone()), None, |tx| {
            tx.execute_batch("UPDATE notes SET title='Local title', _updated_at='0000000004000-0000-owner' WHERE id='shared'")?;
            Ok::<_, coven_database::DbError>(())
        }).await.unwrap();
    fixture.snapshot_peer_edit(false).await;
    let mut migrations = test_migrations();
    migrations.push(
        Migration::sql(
            2,
            "note category",
            "ALTER TABLE notes ADD COLUMN category TEXT NOT NULL DEFAULT 'note';",
        )
        .changesets(vec![TableChangesetMigration::new(
            "notes",
            &[],
            |row, _| {
                match &mut row.change {
                    ChangesetOperation::Insert(columns) | ChangesetOperation::Delete(columns) => {
                        columns.push(ChangesetColumn {
                            name: "category".into(),
                            primary_key: false,
                            value: rusqlite::types::Value::Text("note".into()),
                        });
                    }
                    ChangesetOperation::Update(columns) => {
                        columns.push(ChangesetColumn {
                            name: "category".into(),
                            primary_key: false,
                            value: ChangesetUpdate {
                                old: None,
                                new: None,
                            },
                        });
                    }
                }
                Ok(())
            },
        )]),
    );
    let upgraded = RebaseFixture::open_schema(
        &fixture.path,
        fixture.source_dir.clone(),
        test_synced_tables(),
        &migrations,
    );
    let owner = fixture
        .store
        .bind_device_in(&upgraded, fixture.source_dir.clone(), &fixture.signer)
        .await
        .unwrap();
    owner
        .pull_store()
        .await
        .expect("capture actual rebase under schema two");
    assert_eq!(
        coven_database::DatabaseImageTest::open(&fixture.path)
            .unwrap()
            .store_write_schema_versions(&receipt.write_id)
            .unwrap(),
        (1, Some(2))
    );
    drop(owner);
    drop(upgraded);
    if legacy {
        let image = coven_database::DatabaseImageTest::open(&fixture.path).unwrap();
        image.downgrade_coven_schema_to_v2().unwrap();
    }
    migrations.push(
        Migration::sql(
            3,
            "qualify titles",
            "UPDATE notes SET title='migrated:' || title;",
        )
        .changesets(vec![TableChangesetMigration::new(
            "notes",
            &[],
            |row, _| {
                qualify_title(row);
                Ok(())
            },
        )]),
    );
    let upgraded = RebaseFixture::open_schema(
        &fixture.path,
        fixture.source_dir.clone(),
        test_synced_tables(),
        &migrations,
    );
    let database = StoreDatabase::new(&upgraded);
    database
        .set_write_status(
            &receipt.write_id,
            WriteStatus::Blocked(WriteBlock::InvalidProtocolState {
                reason: "discard rebased edit".into(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        database
            .discard_blocked_write(&receipt.write_id)
            .await
            .unwrap(),
        coven_database::BlockedWriteDiscard::Discarded(vec![receipt.write_id])
    );
    assert_eq!(
        upgraded
            .query_test_text(
                "SELECT title || ':' || body || ':' || category FROM notes WHERE id='shared'"
            )
            .await,
        "migrated:Original title:Peer changed body:note"
    );
}

#[tokio::test]
async fn failed_inverse_conversion_rolls_back_the_discarded_suffix_then_retries() {
    discard_after_value_migration(false, true).await;
}

fn qualify_title(row: &mut ChangesetRow) {
    match &mut row.change {
        ChangesetOperation::Insert(columns) | ChangesetOperation::Delete(columns) => {
            for column in columns.iter_mut().filter(|column| column.name == "title") {
                if let rusqlite::types::Value::Text(text) = &mut column.value {
                    *text = format!("migrated:{text}");
                }
            }
        }
        ChangesetOperation::Update(columns) => {
            for column in columns.iter_mut().filter(|column| column.name == "title") {
                for value in [&mut column.value.old, &mut column.value.new] {
                    if let Some(rusqlite::types::Value::Text(text)) = value {
                        *text = format!("migrated:{text}");
                    }
                }
            }
        }
    }
}
