//! Integration tests for the capture / apply / conflict stack.
//!
//! Run against the synthetic, domain-free schema (`notes` / `note_tags` /
//! `note_photos`) through a real [`coven_database::Database`], so the engine is
//! exercised end-to-end the same way production drives it.

use crate::sync::test_helpers::*;

#[tokio::test]
async fn undeclared_changeset_table_is_rejected() {
    let source = open_test_db();
    source
        .database
        .execute_test_sql(
            "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;",
        )
        .await;
    let changeset = source
        .database
        .capture_test_changeset_for_tables(
            &["local_only"],
            "INSERT INTO local_only (id, value) VALUES ('local-1', 'private');",
        )
        .await;

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;",
        )
        .await;

    let error = target
        .database
        .try_apply_test_changeset(&changeset)
        .await
        .expect_err("an undeclared table must reject the changeset");
    assert!(
        error.to_string().contains("local_only"),
        "rejection must name the undeclared table: {error}"
    );
    assert!(
        !target
            .database
            .test_row_exists("SELECT 1 FROM local_only WHERE id = 'local-1'")
            .await
    );
}

#[tokio::test]
async fn mixed_changeset_with_undeclared_table_is_rejected_atomically() {
    let source = open_test_db();
    source
        .database
        .execute_test_sql(
            "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
         INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-premerge', 'title0', 'body0', '0000000001000-0000-base', '2026-01-01');",
        )
        .await;
    let changeset = source
        .database
        .capture_test_changeset_for_tables(
            &["notes", "local_only"],
            "UPDATE notes \
         SET title = 'incoming-title', _updated_at = '0000000003000-0000-incoming' \
         WHERE id = 'n-premerge';
         INSERT INTO local_only (id, value) VALUES ('local-1', 'private');",
        )
        .await;

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
         INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-premerge', 'title0', 'body0', '0000000001000-0000-base', '2026-01-01');
         UPDATE notes \
         SET body = 'local-body', _updated_at = '0000000005000-0000-local' \
         WHERE id = 'n-premerge';",
        )
        .await;

    let result = target.database.try_apply_test_changeset(&changeset).await;
    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n-premerge'")
            .await,
        "title0",
        "the losing UPDATE must not premerge before table validation finishes"
    );
    assert!(
        !target
            .database
            .test_row_exists("SELECT 1 FROM local_only WHERE id = 'local-1'")
            .await,
        "the undeclared row must not apply"
    );
    let error = result.expect_err("any undeclared table must reject the whole changeset");
    assert!(
        error.to_string().contains("local_only"),
        "rejection must name the undeclared table: {error}"
    );
}

#[tokio::test]
async fn session_captures_and_applies_inserts() {
    let src = open_test_db();
    let cs = src
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'First', 'hello', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
        ])
        .await;
    assert!(!cs.is_empty());

    let target = open_test_db();
    target.database.apply_test_changeset(&cs).await;

    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "First"
    );
    assert_eq!(
        target
            .database
            .query_test_text("SELECT tag FROM note_tags WHERE id = 't1'")
            .await,
        "green"
    );
}

#[tokio::test]
async fn shared_key_inserts_with_equal_ids_converge_in_both_apply_orders() {
    let older_source = open_test_db();
    let older = older_source
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('preferences', 'older', NULL, 1, '0000000001000-0000-a', '2026-01-01')",
        ])
        .await;
    let newer_source = open_test_db();
    let newer = newer_source
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('preferences', 'newer', NULL, 1, '0000000002000-0000-b', '2026-01-01')",
        ])
        .await;

    let older_then_newer = open_test_db();
    older_then_newer.database.apply_test_changeset(&older).await;
    older_then_newer.database.apply_test_changeset(&newer).await;
    let newer_then_older = open_test_db();
    newer_then_older.database.apply_test_changeset(&newer).await;
    newer_then_older.database.apply_test_changeset(&older).await;

    for target in [&older_then_newer, &newer_then_older] {
        assert_eq!(
            target
                .database
                .query_test_text("SELECT title FROM notes WHERE id = 'preferences'")
                .await,
            "newer"
        );
        assert_eq!(
            target
                .database
                .query_test_text("SELECT _updated_at FROM notes WHERE id = 'preferences'")
                .await,
            "0000000002000-0000-b"
        );
    }
}

#[tokio::test]
async fn lww_later_update_wins() {
    // Source builds an UPDATE changeset from base ts=1 to ts=9.
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    // Drain the insert capture so the changeset is just the UPDATE.
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&[
            "UPDATE notes SET title = 'B', _updated_at = '0000000009000-0000-s' WHERE id = 'n1'",
        ])
        .await;

    // Target has its own edit at ts=5 (older than the incoming ts=9).
    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000005000-0000-t', '2026-01-01')",
        )
        .await;
    target.database.apply_test_changeset(&cs).await;

    // Incoming ts=9 > local ts=5, so the incoming title wins.
    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "B"
    );
}

#[tokio::test]
async fn lww_earlier_update_loses() {
    // Source builds an UPDATE changeset from base ts=1 to ts=3 (older).
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&[
            "UPDATE notes SET title = 'B', _updated_at = '0000000003000-0000-s' WHERE id = 'n1'",
        ])
        .await;

    // Target's edit at ts=5 is newer than the incoming ts=3.
    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'LOCAL', NULL, '0000000005000-0000-t', '2026-01-01')",
        )
        .await;
    target.database.apply_test_changeset(&cs).await;

    // Incoming ts=3 < local ts=5, so the local title is kept.
    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "LOCAL"
    );

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'LOCAL', NULL, '0000000005000-0000-t', '2026-01-01')",
        )
        .await;
    let bytes = cs.clone();
    let tables = test_synced_tables();
    let receiver_wall_ms = target.database.receive_wall_ms();
    let winners = target
        .database
        .test_sql(move |database| {
            database
                .apply_changeset(&bytes, &tables, receiver_wall_ms)
                .map(|result| result.winning_rows)
        })
        .await
        .expect("apply local-winning update");
    assert!(
        winners.is_empty(),
        "a local-winning row must not authorize replacement of its exact blob binding"
    );
}

#[tokio::test]
async fn independent_column_edits_converge() {
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&["UPDATE notes \
             SET title = 'titleA', _updated_at = '0000000003000-0000-s' \
             WHERE id = 'n1'"])
        .await;

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01');
         UPDATE notes \
         SET body = 'bodyB', _updated_at = '0000000005000-0000-t' \
         WHERE id = 'n1';",
        )
        .await;

    target.database.apply_test_changeset(&cs).await;

    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "titleA"
    );
    assert_eq!(
        target
            .database
            .query_test_text("SELECT body FROM notes WHERE id = 'n1'")
            .await,
        "bodyB"
    );
}

#[tokio::test]
async fn same_column_contention_keeps_newer() {
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&["UPDATE notes \
             SET title = 'titleA', _updated_at = '0000000003000-0000-s' \
             WHERE id = 'n1'"])
        .await;

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01');
         UPDATE notes \
         SET title = 'titleB', _updated_at = '0000000005000-0000-t' \
         WHERE id = 'n1';",
        )
        .await;

    target.database.apply_test_changeset(&cs).await;

    assert_eq!(
        target
            .database
            .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
            .await,
        "titleB"
    );
    assert_eq!(
        target
            .database
            .query_test_text("SELECT body FROM notes WHERE id = 'n1'")
            .await,
        "body0"
    );
}

/// Seed a fresh source with `base`, drain the insert capture, then capture a
/// single UPDATE — the changeset a writer who started from that base would push.
async fn update_from_base(base: &str, update: &str) -> Vec<u8> {
    let src = open_test_db();
    src.database.execute_test_sql(base).await;
    let _ = src.database.capture_test_changeset(&[]).await;
    src.database.capture_test_changeset(&[update]).await
}

/// Three writers contend on one column while a fourth wins the row on a *different*
/// column, and the final value of the contended column depends on apply order.
///
/// Raw changeset application is order-dependent because the schema carries one
/// per-row `_updated_at`, not a per-column stamp. The Store protocol compensates
/// by replaying late concurrent accepted history in its canonical order. This
/// test pins the lower-level behavior that makes that replay necessary.
///
/// If this assertion fails, re-evaluate whether Store replay is still required.
#[tokio::test]
async fn raw_three_writer_same_column_contention_is_order_dependent() {
    // base c0; X1 and X2 both edit `title` away from c0 concurrently; M wins the
    // row on `body` with the highest stamp (9000 > both 3000 and 4000).
    let base = "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                VALUES ('n1', 'c0', 'b0', '0000000001000-0000-base', '2026-01-01')";
    let x1 = update_from_base(
        base,
        "UPDATE notes SET title = 'c1', _updated_at = '0000000003000-0000-x1' WHERE id = 'n1'",
    )
    .await;
    let x2 = update_from_base(
        base,
        "UPDATE notes SET title = 'c2', _updated_at = '0000000004000-0000-x2' WHERE id = 'n1'",
    )
    .await;
    let m = update_from_base(
        base,
        "UPDATE notes SET body = 'bM', _updated_at = '0000000009000-0000-m' WHERE id = 'n1'",
    )
    .await;

    // Apply order X1, M, X2.
    let a = open_test_db();
    a.database.execute_test_sql(base).await;
    a.database.apply_test_changeset(&x1).await;
    a.database.apply_test_changeset(&m).await;
    a.database.apply_test_changeset(&x2).await;

    // Apply order X2, M, X1.
    let b = open_test_db();
    b.database.execute_test_sql(base).await;
    b.database.apply_test_changeset(&x2).await;
    b.database.apply_test_changeset(&m).await;
    b.database.apply_test_changeset(&x1).await;

    // The row winner's column and stamp converge regardless of order.
    for db in [&a, &b] {
        assert_eq!(
            db.database
                .query_test_text("SELECT body FROM notes WHERE id = 'n1'")
                .await,
            "bM"
        );
        assert_eq!(
            db.database
                .query_test_text("SELECT _updated_at FROM notes WHERE id = 'n1'")
                .await,
            "0000000009000-0000-m"
        );
    }

    // The contended column does not converge: whichever same-column writer landed
    // first (before the row winner moved the row past its base) keeps its value.
    let title_a = a
        .database
        .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
        .await;
    let title_b = b
        .database
        .query_test_text("SELECT title FROM notes WHERE id = 'n1'")
        .await;
    assert_eq!(title_a, "c1");
    assert_eq!(title_b, "c2");
    assert_ne!(
        title_a, title_b,
        "raw 3-writer same-column application must remain the order-dependent case \
         covered by Store canonical replay"
    );
}

#[tokio::test]
async fn fk_violation_is_reported_then_resolved_on_retry() {
    // Capture a child insert (note_tags -> notes) on a source that has the parent.
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    let _ = src.database.capture_test_changeset(&[]).await;
    let child_cs = src
        .database
        .capture_test_changeset(&[
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
           VALUES ('t1', 'n1', 'green', '0000000002000-0000-s', '2026-01-01')",
        ])
        .await;

    let parent_src = open_test_db();
    let parent_cs = parent_src
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        ])
        .await;

    // Apply child first on an empty target: FK violation flagged.
    let target = open_test_db();
    let r1 = target
        .database
        .apply_test_changeset_reporting_foreign_key_violations(&child_cs)
        .await
        .expect("apply child changeset");
    assert!(r1, "child without parent violates FK");

    // Apply parent, then re-apply child: now it resolves.
    target.database.apply_test_changeset(&parent_cs).await;
    let r2 = target
        .database
        .apply_test_changeset_reporting_foreign_key_violations(&child_cs)
        .await
        .expect("reapply child changeset");
    assert!(!r2);
    assert_eq!(
        target
            .database
            .query_test_text("SELECT tag FROM note_tags WHERE id = 't1'")
            .await,
        "green"
    );
}

#[tokio::test]
async fn caller_owned_transaction_can_resolve_fk_violation_with_a_later_changeset() {
    let source = open_test_db();
    source
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        )
        .await;
    let _ = source.database.capture_test_changeset(&[]).await;
    let child = source
        .database
        .capture_test_changeset(&[
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
           VALUES ('t1', 'n1', 'green', '0000000002000-0000-s', '2026-01-01')",
        ])
        .await;

    let parent_source = open_test_db();
    let parent = parent_source
        .database
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        ])
        .await;

    let target = open_test_db();
    let tables = test_synced_tables();
    let receiver_wall_ms = target.database.receive_wall_ms();
    target
        .database
        .test_sql(move |database| {
            let (results, violations) = database.apply_changesets_atomically(
                vec![child, parent],
                &tables,
                receiver_wall_ms,
            )?;
            assert!(results[0].had_fk_violations);
            assert!(!results[1].had_fk_violations);
            assert!(!violations);
            Ok(())
        })
        .await
        .expect("apply dependent changesets atomically");

    assert_eq!(
        target
            .database
            .query_test_text("SELECT tag FROM note_tags WHERE id = 't1'")
            .await,
        "green"
    );
}

#[tokio::test]
async fn delete_applies() {
    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
        )
        .await;

    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
        )
        .await;
    // Drain the insert capture so the changeset is just the DELETE (an INSERT +
    // DELETE of the same row in one session nets to no change).
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&["DELETE FROM notes WHERE id = 'n1'"])
        .await;

    target.database.apply_test_changeset(&cs).await;
    assert!(
        !target
            .database
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
}

#[tokio::test]
async fn concurrent_delete_and_update_converge_to_deleted() {
    let src = open_test_db();
    src.database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000005000-0000-d', '2026-01-01')",
        )
        .await;
    let _ = src.database.capture_test_changeset(&[]).await;
    let cs = src
        .database
        .capture_test_changeset(&["DELETE FROM notes WHERE id = 'n1'"])
        .await;

    let target = open_test_db();
    target
        .database
        .execute_test_sql(
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000005000-0000-d', '2026-01-01');
         UPDATE notes \
         SET title = 'local update', _updated_at = '0000000020000-0000-u' \
         WHERE id = 'n1';",
        )
        .await;

    target.database.apply_test_changeset(&cs).await;

    assert!(
        !target
            .database
            .test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'")
            .await
    );
}
