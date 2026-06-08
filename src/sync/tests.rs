//! Integration tests for the capture / apply / conflict stack.
//!
//! Run against the synthetic, domain-free schema (`notes` / `note_tags` /
//! `note_photos`) through a real [`crate::database::Database`], so the engine is
//! exercised end-to-end the same way production drives it.

use crate::sync::test_helpers::*;

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

/// Apply a changeset and report whether it had FK violations, mirroring the
/// suspend/apply/resume lifecycle of [`apply_to_db`].
async fn apply_reporting(db: &crate::database::Database, bytes: &[u8]) -> bool {
    use crate::sync::apply::apply_changeset_lww;
    db.take_changeset_and_suspend().await.expect("suspend");
    let bytes = bytes.to_vec();
    let tables = test_synced_tables();
    let had = db
        .call(move |conn| apply_changeset_lww(conn, &bytes, &tables).map(|r| r.had_fk_violations))
        .await
        .expect("apply");
    db.resume_session().await.expect("resume");
    had
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
