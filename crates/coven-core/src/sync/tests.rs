//! Integration tests for the capture / apply / conflict stack.
//!
//! Run against the synthetic, domain-free schema (`notes` / `note_tags` /
//! `note_photos`) through a real [`crate::database::Database`], so the engine is
//! exercised end-to-end the same way production drives it.

use crate::sync::test_helpers::*;

async fn raw_changeset(db: &crate::database::Database, tables: &[&str], sql: &str) -> Vec<u8> {
    let tables = tables
        .iter()
        .map(|table| table.to_string())
        .collect::<Vec<_>>();
    let sql = sql.to_string();
    db.call(move |conn| {
        let mut session =
            rusqlite::session::Session::new(conn).map_err(crate::database::DbError::from)?;
        for table in tables {
            session
                .attach(Some(table.as_str()))
                .map_err(crate::database::DbError::from)?;
        }
        conn.execute_batch(&sql)
            .map_err(crate::database::DbError::from)?;
        let mut bytes = Vec::new();
        session
            .changeset_strm(&mut bytes)
            .map_err(crate::database::DbError::from)?;
        Ok(bytes)
    })
    .await
    .expect("capture raw changeset")
}

async fn apply_result(
    db: &crate::database::Database,
    bytes: &[u8],
) -> Result<(), crate::database::DbError> {
    use crate::sync::apply::resolve_and_apply_changeset;

    let bytes = bytes.to_vec();
    let tables = test_synced_tables();
    let receiver_wall_ms = db.receive_wall_ms();
    db.call(move |conn| {
        resolve_and_apply_changeset(conn, &bytes, &tables, receiver_wall_ms)
            .map(|_| ())
            .map_err(|error| crate::database::DbError(error.to_string()))
    })
    .await
}

#[tokio::test]
async fn undeclared_changeset_table_is_rejected() {
    let source = open_test_db();
    exec(
        &source,
        "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;",
    )
    .await;
    let changeset = raw_changeset(
        &source,
        &["local_only"],
        "INSERT INTO local_only (id, value) VALUES ('local-1', 'private');",
    )
    .await;

    let target = open_test_db();
    exec(
        &target,
        "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;",
    )
    .await;

    let error = apply_result(&target, &changeset)
        .await
        .expect_err("an undeclared table must reject the changeset");
    assert!(
        error.to_string().contains("local_only"),
        "rejection must name the undeclared table: {error}"
    );
    assert!(!row_exists(&target, "SELECT 1 FROM local_only WHERE id = 'local-1'").await);
}

#[tokio::test]
async fn mixed_changeset_with_undeclared_table_is_rejected_atomically() {
    let source = open_test_db();
    exec(
        &source,
        "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
         INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-premerge', 'title0', 'body0', '0000000001000-0000-base', '2026-01-01');",
    )
    .await;
    let changeset = raw_changeset(
        &source,
        &["notes", "local_only"],
        "UPDATE notes \
         SET title = 'incoming-title', _updated_at = '0000000003000-0000-incoming' \
         WHERE id = 'n-premerge';
         INSERT INTO local_only (id, value) VALUES ('local-1', 'private');",
    )
    .await;

    let target = open_test_db();
    exec(
        &target,
        "CREATE TABLE local_only (id TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
         INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n-premerge', 'title0', 'body0', '0000000001000-0000-base', '2026-01-01');
         UPDATE notes \
         SET body = 'local-body', _updated_at = '0000000005000-0000-local' \
         WHERE id = 'n-premerge';",
    )
    .await;

    let result = apply_result(&target, &changeset).await;
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n-premerge'").await,
        "title0",
        "the losing UPDATE must not premerge before table validation finishes"
    );
    assert!(
        !row_exists(&target, "SELECT 1 FROM local_only WHERE id = 'local-1'").await,
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
    let cs = capture_bytes(
        &src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'First', 'hello', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    )
    .await;
    assert!(!cs.is_empty());

    let target = open_test_db();
    apply_to_db(&target, &cs, &test_synced_tables()).await;

    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n1'").await,
        "First"
    );
    assert_eq!(
        query_text(&target, "SELECT tag FROM note_tags WHERE id = 't1'").await,
        "green"
    );
}

#[tokio::test]
async fn lww_later_update_wins() {
    // Source builds an UPDATE changeset from base ts=1 to ts=9.
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    // Drain the insert capture so the changeset is just the UPDATE.
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(
        &src,
        &["UPDATE notes SET title = 'B', _updated_at = '0000000009000-0000-s' WHERE id = 'n1'"],
    )
    .await;

    // Target has its own edit at ts=5 (older than the incoming ts=9).
    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000005000-0000-t', '2026-01-01')",
    )
    .await;
    apply_to_db(&target, &cs, &test_synced_tables()).await;

    // Incoming ts=9 > local ts=5, so the incoming title wins.
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n1'").await,
        "B"
    );
}

#[tokio::test]
async fn lww_earlier_update_loses() {
    // Source builds an UPDATE changeset from base ts=1 to ts=3 (older).
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(
        &src,
        &["UPDATE notes SET title = 'B', _updated_at = '0000000003000-0000-s' WHERE id = 'n1'"],
    )
    .await;

    // Target's edit at ts=5 is newer than the incoming ts=3.
    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'LOCAL', NULL, '0000000005000-0000-t', '2026-01-01')",
    )
    .await;
    apply_to_db(&target, &cs, &test_synced_tables()).await;

    // Incoming ts=3 < local ts=5, so the local title is kept.
    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n1'").await,
        "LOCAL"
    );
}

#[tokio::test]
async fn losing_stale_update_does_not_replace_the_active_blob_location() {
    use std::sync::Arc;

    use crate::blob::{CacheFill, CloudBlobLocation, Provenance};
    use crate::sync::apply::{resolve_and_apply_changeset_with_schema, BlobLocationAssignment};
    use crate::sync::conflict::TableSchema;
    use crate::sync::session::BlobDecl;

    let decl = BlobDecl::new("photos", Provenance::UserProvided, CacheFill::CacheLazy);
    let tables = test_synced_tables_with_blob(decl.clone());
    let source = open_test_db_with_blob(decl.clone());
    exec(
        &source,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'root', NULL, '0000000001000-0000-s', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1', 'n1', 'old', '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&source, &[]).await;
    let stale = capture_bytes(
        &source,
        &["UPDATE note_photos SET kind = 'stale', \
           _updated_at = '0000000002000-0000-s' WHERE id = 'p1'"],
    )
    .await;

    let target = open_test_db_with_blob(decl);
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'root', NULL, '0000000001000-0000-t', '2026-01-01'); \
         INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
         VALUES ('p1', 'n1', 'newer', '0000000003000-0000-t', '2026-01-01')",
    )
    .await;
    let current = CloudBlobLocation::generated("new-uploader", "0000000003000-0000-t");
    target
        .record_blob_location("photos", "p1", &current)
        .await
        .unwrap();
    let incoming = CloudBlobLocation::generated("stale-uploader", "0000000002000-0000-s");
    let receiver_wall_ms = target.receive_wall_ms();
    let applied_incoming = incoming.clone();
    target
        .call(move |conn| {
            let table_names: Vec<&str> = tables.iter().map(|table| table.name()).collect();
            let schema = Arc::new(TableSchema::from_db(conn, &table_names)?);
            resolve_and_apply_changeset_with_schema(
                conn,
                &stale,
                schema,
                receiver_wall_ms,
                &[BlobLocationAssignment {
                    table: "note_photos".to_string(),
                    pk: "p1".to_string(),
                    row_version: "0000000002000-0000-s".to_string(),
                    namespace: "photos".to_string(),
                    blob_id: "p1".to_string(),
                    location: applied_incoming,
                }],
            )
            .map(|_| ())
            .map_err(|error| crate::database::DbError(error.to_string()))
        })
        .await
        .unwrap();

    assert_eq!(
        query_text(&target, "SELECT kind FROM note_photos WHERE id = 'p1'").await,
        "newer",
    );
    assert_eq!(
        target.blob_location("photos", "p1").await.unwrap(),
        Some(current)
    );
}

#[tokio::test]
async fn independent_column_edits_converge() {
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(
        &src,
        &["UPDATE notes \
             SET title = 'titleA', _updated_at = '0000000003000-0000-s' \
             WHERE id = 'n1'"],
    )
    .await;

    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01');
         UPDATE notes \
         SET body = 'bodyB', _updated_at = '0000000005000-0000-t' \
         WHERE id = 'n1';",
    )
    .await;

    apply_to_db(&target, &cs, &test_synced_tables()).await;

    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n1'").await,
        "titleA"
    );
    assert_eq!(
        query_text(&target, "SELECT body FROM notes WHERE id = 'n1'").await,
        "bodyB"
    );
}

#[tokio::test]
async fn same_column_contention_keeps_newer() {
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(
        &src,
        &["UPDATE notes \
             SET title = 'titleA', _updated_at = '0000000003000-0000-s' \
             WHERE id = 'n1'"],
    )
    .await;

    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000001000-0000-s', '2026-01-01');
         UPDATE notes \
         SET title = 'titleB', _updated_at = '0000000005000-0000-t' \
         WHERE id = 'n1';",
    )
    .await;

    apply_to_db(&target, &cs, &test_synced_tables()).await;

    assert_eq!(
        query_text(&target, "SELECT title FROM notes WHERE id = 'n1'").await,
        "titleB"
    );
    assert_eq!(
        query_text(&target, "SELECT body FROM notes WHERE id = 'n1'").await,
        "body0"
    );
}

/// Seed a fresh source with `base`, drain the insert capture, then capture a
/// single UPDATE — the changeset a writer who started from that base would push.
async fn update_from_base(base: &str, update: &str) -> Vec<u8> {
    let src = open_test_db();
    exec(&src, base).await;
    let _ = capture_bytes(&src, &[]).await;
    capture_bytes(&src, &[update]).await
}

/// Three writers contend on one column while a fourth wins the row on a *different*
/// column, and the final value of the contended column depends on apply order.
///
/// This is a deliberate, documented property of the convergence model, not a bug:
/// the schema carries a single per-row `_updated_at`, not a per-column stamp, so
/// the column-level premerge can only fold a losing writer's column in while the
/// local column still equals that writer's base. Once the row winner (or the other
/// contender) has moved the column off its base, a later losing writer to the same
/// column is dropped. Both orders therefore land a value from the contending set,
/// stamped at the row winner's time, with no reconciling event — resolved only by
/// the next edit to that column. The row winner's own column converges regardless
/// of order; only the contended column diverges, and only among 3+ same-column
/// writers interleaved with a row winner.
///
/// Pinned deliberately: if this assertion fails, same-column convergence semantics
/// changed — confirm the change is intentional before updating the test.
#[tokio::test]
async fn three_writer_same_column_contention_is_order_dependent() {
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
    exec(&a, base).await;
    apply_to_db(&a, &x1, &test_synced_tables()).await;
    apply_to_db(&a, &m, &test_synced_tables()).await;
    apply_to_db(&a, &x2, &test_synced_tables()).await;

    // Apply order X2, M, X1.
    let b = open_test_db();
    exec(&b, base).await;
    apply_to_db(&b, &x2, &test_synced_tables()).await;
    apply_to_db(&b, &m, &test_synced_tables()).await;
    apply_to_db(&b, &x1, &test_synced_tables()).await;

    // The row winner's column and stamp converge regardless of order.
    for db in [&a, &b] {
        assert_eq!(
            query_text(db, "SELECT body FROM notes WHERE id = 'n1'").await,
            "bM"
        );
        assert_eq!(
            query_text(db, "SELECT _updated_at FROM notes WHERE id = 'n1'").await,
            "0000000009000-0000-m"
        );
    }

    // The contended column does not converge: whichever same-column writer landed
    // first (before the row winner moved the row past its base) keeps its value.
    let title_a = query_text(&a, "SELECT title FROM notes WHERE id = 'n1'").await;
    let title_b = query_text(&b, "SELECT title FROM notes WHERE id = 'n1'").await;
    assert_eq!(title_a, "c1");
    assert_eq!(title_b, "c2");
    assert_ne!(
        title_a, title_b,
        "3-writer same-column contention is a deliberately order-dependent property \
         of the single-per-row-stamp model; if these now converge, the convergence \
         semantics changed — confirm that is intentional before updating this test"
    );
}

#[tokio::test]
async fn fk_violation_is_reported_then_resolved_on_retry() {
    // Capture a child insert (note_tags -> notes) on a source that has the parent.
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&src, &[]).await;
    let child_cs = capture_bytes(
        &src,
        &[
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
           VALUES ('t1', 'n1', 'green', '0000000002000-0000-s', '2026-01-01')",
        ],
    )
    .await;

    let parent_src = open_test_db();
    let parent_cs = capture_bytes(
        &parent_src,
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        ],
    )
    .await;

    // Apply child first on an empty target: FK violation flagged.
    let target = open_test_db();
    let r1 = apply_reporting(&target, &child_cs).await;
    assert!(r1, "child without parent violates FK");

    // Apply parent, then re-apply child: now it resolves.
    apply_to_db(&target, &parent_cs, &test_synced_tables()).await;
    let r2 = apply_reporting(&target, &child_cs).await;
    assert!(!r2);
    assert_eq!(
        query_text(&target, "SELECT tag FROM note_tags WHERE id = 't1'").await,
        "green"
    );
}

/// Apply a changeset and report whether it had FK violations, through the same
/// plain-`call` apply path as [`apply_to_db`].
async fn apply_reporting(db: &crate::database::Database, bytes: &[u8]) -> bool {
    use crate::sync::apply::resolve_and_apply_changeset;
    let bytes = bytes.to_vec();
    let tables = test_synced_tables();
    let receiver_wall_ms = db.receive_wall_ms();
    db.call(move |conn| {
        resolve_and_apply_changeset(conn, &bytes, &tables, receiver_wall_ms)
            .map(|r| r.had_fk_violations)
            .map_err(|error| crate::database::DbError(error.to_string()))
    })
    .await
    .expect("apply")
}

#[tokio::test]
async fn delete_applies() {
    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
    )
    .await;

    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
    )
    .await;
    // Drain the insert capture so the changeset is just the DELETE (an INSERT +
    // DELETE of the same row in one session nets to no change).
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(&src, &["DELETE FROM notes WHERE id = 'n1'"]).await;

    apply_to_db(&target, &cs, &test_synced_tables()).await;
    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}

#[tokio::test]
async fn concurrent_delete_and_update_converge_to_deleted() {
    let src = open_test_db();
    exec(
        &src,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000005000-0000-d', '2026-01-01')",
    )
    .await;
    let _ = capture_bytes(&src, &[]).await;
    let cs = capture_bytes(&src, &["DELETE FROM notes WHERE id = 'n1'"]).await;

    let target = open_test_db();
    exec(
        &target,
        "INSERT INTO notes (id, title, body, _updated_at, created_at) \
         VALUES ('n1', 'title0', 'body0', '0000000005000-0000-d', '2026-01-01');
         UPDATE notes \
         SET title = 'local update', _updated_at = '0000000020000-0000-u' \
         WHERE id = 'n1';",
    )
    .await;

    apply_to_db(&target, &cs, &test_synced_tables()).await;

    assert!(!row_exists(&target, "SELECT 1 FROM notes WHERE id = 'n1'").await);
}
