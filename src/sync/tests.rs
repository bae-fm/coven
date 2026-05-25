//! Integration tests for the session / apply / conflict stack.
//!
//! Run against the synthetic, domain-free schema (`notes` / `note_tags` /
//! `note_photos`) from `test_helpers`, using raw sqlite3 connections so the
//! engine is exercised end-to-end without a host `Database`.

use crate::sync::apply::apply_changeset_lww;
use crate::sync::session::{synced_tables, SyncSession};
use crate::sync::session_ext::Changeset;
use crate::sync::test_helpers::*;
use libsqlite3_sys as ffi;

/// Capture a changeset: start a session, run `stmts`, return the diff.
unsafe fn capture(db: *mut ffi::sqlite3, stmts: &[&str]) -> Option<Changeset> {
    let session = SyncSession::start(db).expect("start session");
    for s in stmts {
        exec(db, s);
    }
    session.changeset().expect("changeset")
}

#[test]
fn synced_tables_are_configured() {
    init_synced_tables();
    let tables = synced_tables();
    assert!(tables.iter().any(|t| t == "notes"));
    assert!(tables.iter().any(|t| t == "note_tags"));
    assert!(tables.iter().any(|t| t == "note_photos"));
}

#[test]
fn session_captures_and_applies_inserts() {
    unsafe {
        init_synced_tables();
        let db = open_memory_db();
        create_synced_schema(db);

        let cs = capture(
            db,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('n1', 'First', 'hello', '0000000001000-0000-dev1', '2026-01-01')",
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            ],
        )
        .expect("should have changes");
        assert!(!cs.is_empty());

        let db2 = open_memory_db();
        create_synced_schema(db2);
        let result = apply_changeset_lww(db2, &cs).expect("apply");
        assert!(!result.had_fk_violations);

        assert_eq!(
            query_text(db2, "SELECT title FROM notes WHERE id = 'n1'"),
            "First"
        );
        assert_eq!(
            query_text(db2, "SELECT tag FROM note_tags WHERE id = 't1'"),
            "green"
        );

        ffi::sqlite3_close(db);
        ffi::sqlite3_close(db2);
    }
}

#[test]
fn lww_later_update_wins() {
    unsafe {
        init_synced_tables();
        // Source builds an UPDATE changeset from base ts=1 to ts=9.
        let src = open_memory_db();
        create_synced_schema(src);
        exec(
            src,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
        );
        let cs = capture(
            src,
            &["UPDATE notes SET title = 'B', _updated_at = '0000000009000-0000-s' WHERE id = 'n1'"],
        )
        .expect("cs");

        // Target has its own edit at ts=5 (older than the incoming ts=9).
        let target = open_memory_db();
        create_synced_schema(target);
        exec(
            target,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A', NULL, '0000000005000-0000-t', '2026-01-01')",
        );
        apply_changeset_lww(target, &cs).expect("apply");

        // Incoming ts=9 > local ts=5, so the incoming title wins.
        assert_eq!(
            query_text(target, "SELECT title FROM notes WHERE id = 'n1'"),
            "B"
        );

        ffi::sqlite3_close(src);
        ffi::sqlite3_close(target);
    }
}

#[test]
fn lww_earlier_update_loses() {
    unsafe {
        init_synced_tables();
        // Source builds an UPDATE changeset from base ts=1 to ts=3 (older).
        let src = open_memory_db();
        create_synced_schema(src);
        exec(
            src,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'A', NULL, '0000000001000-0000-s', '2026-01-01')",
        );
        let cs = capture(
            src,
            &["UPDATE notes SET title = 'B', _updated_at = '0000000003000-0000-s' WHERE id = 'n1'"],
        )
        .expect("cs");

        // Target's edit at ts=5 is newer than the incoming ts=3.
        let target = open_memory_db();
        create_synced_schema(target);
        exec(
            target,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'LOCAL', NULL, '0000000005000-0000-t', '2026-01-01')",
        );
        apply_changeset_lww(target, &cs).expect("apply");

        // Incoming ts=3 < local ts=5, so the local title is kept.
        assert_eq!(
            query_text(target, "SELECT title FROM notes WHERE id = 'n1'"),
            "LOCAL"
        );

        ffi::sqlite3_close(src);
        ffi::sqlite3_close(target);
    }
}

#[test]
fn fk_violation_is_reported_then_resolved_on_retry() {
    unsafe {
        init_synced_tables();
        // Capture a child insert (note_tags -> notes) on a source that has the parent.
        let src = open_memory_db();
        create_synced_schema(src);
        exec(
            src,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
        );
        let child_cs = capture(
            src,
            &[
                "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
               VALUES ('t1', 'n1', 'green', '0000000002000-0000-s', '2026-01-01')",
            ],
        )
        .expect("child cs");

        let parent_src = open_memory_db();
        create_synced_schema(parent_src);
        let parent_cs = capture(
            parent_src,
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('n1', 'Parent', NULL, '0000000001000-0000-s', '2026-01-01')",
            ],
        )
        .expect("parent cs");

        // Apply child first on an empty target: FK violation flagged.
        let target = open_memory_db();
        create_synced_schema(target);
        let r1 = apply_changeset_lww(target, &child_cs).expect("apply child");
        assert!(r1.had_fk_violations, "child without parent violates FK");

        // Apply parent, then re-apply child: now it resolves.
        apply_changeset_lww(target, &parent_cs).expect("apply parent");
        let r2 = apply_changeset_lww(target, &child_cs).expect("retry child");
        assert!(!r2.had_fk_violations);
        assert_eq!(
            query_text(target, "SELECT tag FROM note_tags WHERE id = 't1'"),
            "green"
        );

        ffi::sqlite3_close(src);
        ffi::sqlite3_close(parent_src);
        ffi::sqlite3_close(target);
    }
}

#[test]
fn delete_applies() {
    unsafe {
        init_synced_tables();
        let target = open_memory_db();
        create_synced_schema(target);
        exec(
            target,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
        );

        let src = open_memory_db();
        create_synced_schema(src);
        exec(
            src,
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('n1', 'Doomed', NULL, '0000000001000-0000-t', '2026-01-01')",
        );
        let cs = capture(src, &["DELETE FROM notes WHERE id = 'n1'"]).expect("cs");

        apply_changeset_lww(target, &cs).expect("apply");
        assert!(!row_exists(target, "SELECT 1 FROM notes WHERE id = 'n1'"));

        ffi::sqlite3_close(src);
        ffi::sqlite3_close(target);
    }
}
