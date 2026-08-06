use super::*;
use crate::database::resolve_and_apply_changeset;
use crate::database::walk_changeset as walk;
use coven_foundation::changeset::ChangeOp;
use rusqlite::session::Session as RqSession;
use rusqlite::Connection;

use super::model::TableGate;
use super::outbound::{gate_outbound, with_empty_clone};
use crate::protocol::synced_schema::SyncedTable;

/// A throwaway in-memory connection with `foreign_keys=ON`, for the gate
/// tests' bespoke schemas. The gate's public API takes `&Connection`.
fn conn() -> Connection {
    let c = Connection::open_in_memory().expect("open in-memory");
    c.execute_batch("PRAGMA foreign_keys = ON").expect("fk on");
    c
}

trait ConnectionTestSql {
    fn execute_test_sql(&self, sql: &str);
    fn apply_test_changeset(&self, bytes: &[u8], tables: &[SyncedTable]);
    fn query_test_text(&self, sql: &str) -> String;
    fn test_row_exists(&self, sql: &str) -> bool;
}

impl ConnectionTestSql for Connection {
    fn execute_test_sql(&self, sql: &str) {
        self.execute_batch(sql)
            .unwrap_or_else(|error| panic!("exec failed for {sql}: {error}"));
    }

    fn apply_test_changeset(&self, bytes: &[u8], tables: &[SyncedTable]) {
        resolve_and_apply_changeset(
            self,
            bytes,
            tables,
            crate::protocol::hlc::now_wall_ms(&coven_foundation::clock::SystemClock),
        )
        .expect("apply test changeset");
    }

    fn query_test_text(&self, sql: &str) -> String {
        self.query_row(sql, [], |row| row.get::<_, String>(0))
            .unwrap_or_else(|error| panic!("query_text failed for {sql}: {error}"))
    }

    fn test_row_exists(&self, sql: &str) -> bool {
        use rusqlite::OptionalExtension;
        self.query_row(sql, [], |_| Ok(()))
            .optional()
            .unwrap_or_else(|error| panic!("row_exists failed for {sql}: {error}"))
            .is_some()
    }
}

fn query_int(c: &Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |r| r.get::<_, i64>(0))
        .unwrap_or_else(|e| panic!("query_int failed for {sql}: {e}"))
}

#[test]
fn scoped_descendant_requires_a_declared_audience_parent() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE note_tags (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL REFERENCES notes(id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );
    let tables = vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .scoped_by("audience"),
        SyncedTable::new(
            "note_tags",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];

    let error = match Gates::from_tables(&c, &tables) {
        Ok(_) => panic!("a scoped descendant must select its audience-parent foreign key"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("must declare its audience-parent foreign key"),
        "{error}"
    );

    let gates = Gates::from_tables(
        &c,
        &[
            SyncedTable::new(
                "notes",
                crate::protocol::synced_schema::RowIdentity::IndependentUuid,
            )
            .scoped_by("audience"),
            SyncedTable::new(
                "note_tags",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .inherits_audience_through("note_id"),
        ],
    )
    .expect("build explicitly declared audience inheritance");
    assert!(gates.tables.contains_key("notes"));
    assert!(gates.tables.contains_key("note_tags"));
}

#[test]
fn child_gate_follows_the_foreign_keys_named_parent_column() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE parents (
                id TEXT PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE children (
                id TEXT PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES parents(code),
                _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO parents VALUES
                ('code-b', 'code-a', 1, '0000000000001-0000-test'),
                ('id-b', 'code-b', 0, '0000000000002-0000-test');
             INSERT INTO children VALUES
                ('child-b', 'code-b', '0000000000003-0000-test');",
    );
    let tables = vec![
        SyncedTable::new(
            "parents",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "children",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];

    let gates = Gates::from_tables(&c, &tables).expect("build gate model");

    assert!(
            !gates
                .row_kept(&c, "children", "child-b")
                .expect("resolve child gate"),
            "the child belongs to the gated-false row whose code the FK names, not the unrelated row whose id happens to equal that code",
        );
}

/// Capture a changeset over `tables` while running `stmts`. Returns the
/// recorded changeset bytes.
fn capture(c: &Connection, tables: &[SyncedTable], stmts: &[&str]) -> Vec<u8> {
    let mut session = RqSession::new(c).expect("session");
    for t in tables {
        session.attach(Some(t.name())).expect("attach");
    }
    for s in stmts {
        c.execute_test_sql(s);
    }
    let mut buf = Vec::new();
    session.changeset_strm(&mut buf).expect("changeset");
    buf
}

/// Capture, then gate against `tables`' gate model. Returns gated bytes.
fn capture_and_gate(c: &Connection, tables: &[SyncedTable], stmts: &[&str]) -> Vec<u8> {
    let bytes = capture(c, tables, stmts);
    let gates = Gates::from_tables(c, tables).expect("build gates");
    gate_outbound(c, &bytes, &gates).expect("gate outbound")
}

fn test_synced_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "note_tags",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "note_photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ]
}

/// The synthetic notes/note_tags/note_photos schema, built directly on `c`.
fn create_synced_schema(c: &Connection) {
    c.execute_test_sql(
        "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE note_tags (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            ) STRICT;
            CREATE TABLE note_photos (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            ) STRICT;",
    );
}

fn create_remote_root_schema(c: &Connection) {
    c.execute_test_sql(
        "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE note_photos (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            ) STRICT;",
    );
}

fn has_row(changes: &[coven_foundation::changeset::RowChange], table: &str, pk: &str) -> bool {
    changes
        .iter()
        .any(|c| c.table == table && c.pk() == Some(pk))
}

#[test]
fn gated_false_root_is_cut() {
    let c = conn();
    create_synced_schema(&c);
    let out = capture_and_gate(
        &c,
        &test_synced_tables(),
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    let changes = walk(&out).expect("walk");
    assert!(
        !has_row(&changes, "notes", "n1"),
        "a gated-false root must be cut from the outbound changeset"
    );
}

#[test]
fn gated_true_root_passes_through() {
    let c = conn();
    create_synced_schema(&c);
    let out = capture_and_gate(
        &c,
        &test_synced_tables(),
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    let changes = walk(&out).expect("walk");
    assert!(
        has_row(&changes, "notes", "n1"),
        "a gated-true root must pass through"
    );
}

#[test]
fn child_cut_because_parent_gated_false() {
    let c = conn();
    create_synced_schema(&c);
    let out = capture_and_gate(
        &c,
        &test_synced_tables(),
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', NULL, 0, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    let changes = walk(&out).expect("walk");
    assert!(!has_row(&changes, "notes", "n1"), "parent cut");
    assert!(
        !has_row(&changes, "note_tags", "t1"),
        "child must be cut because its parent is gated-false"
    );
}

#[test]
fn child_passes_when_parent_gated_true() {
    let c = conn();
    create_synced_schema(&c);
    let out = capture_and_gate(
        &c,
        &test_synced_tables(),
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    let changes = walk(&out).expect("walk");
    assert!(has_row(&changes, "notes", "n1"));
    assert!(
        has_row(&changes, "note_tags", "t1"),
        "child inherits parent's true gate"
    );
    assert!(
        has_row(&changes, "note_photos", "p1"),
        "FK child inherits parent's true gate"
    );
}

#[test]
fn ungated_table_always_passes() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE settings (id TEXT PRIMARY KEY, val TEXT, _updated_at TEXT NOT NULL) STRICT",
    );
    let tables = vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "settings",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];
    let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO notes (id, shared, _updated_at) VALUES ('n1', 0, '0000000001000-0000-dev1')",
                "INSERT INTO settings (id, val, _updated_at) VALUES ('s1', 'x', '0000000001000-0000-dev1')",
            ],
        );
    let changes = walk(&out).expect("walk");
    assert!(!has_row(&changes, "notes", "n1"), "gated-false note is cut");
    assert!(
        has_row(&changes, "settings", "s1"),
        "ungated table always passes through"
    );
}

#[test]
fn remote_root_table_passes_rows_without_gate_column() {
    let c = conn();
    create_remote_root_schema(&c);
    let tables = vec![SyncedTable::new(
        "notes",
        crate::protocol::synced_schema::RowIdentity::SharedKey,
    )
    .remote_root()];
    let out = capture_and_gate(
        &c,
        &tables,
        &["INSERT INTO notes (id, title, _updated_at) \
              VALUES ('n1', 'Remote Root', '0000000001000-0000-dev1')"],
    );
    let changes = walk(&out).expect("walk");
    assert!(
        has_row(&changes, "notes", "n1"),
        "a remote-root row syncs without a gate column"
    );
}

#[test]
fn remote_root_child_resolves_as_remote_and_belongs_to_root_subtree() {
    let c = conn();
    create_remote_root_schema(&c);
    c.execute_test_sql(
        "INSERT INTO notes (id, title, _updated_at) \
             VALUES ('n1', 'Remote Root', '0000000001000-0000-dev1')",
    );
    c.execute_test_sql(
        "INSERT INTO note_photos (id, note_id, _updated_at) \
             VALUES ('p1', 'n1', '0000000001000-0000-dev1')",
    );
    let tables = vec![
        SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .remote_root(),
        SyncedTable::new(
            "note_photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];
    let gates = Gates::from_tables(&c, &tables).expect("gates");

    assert_eq!(
        gates.resolve_root_of(&c, "note_photos", "p1").unwrap(),
        Some(("notes".to_string(), "n1".to_string())),
        "a child resolves to its remote root"
    );
    assert_eq!(
        gates.root_kept_of(&c, "note_photos", "p1").unwrap(),
        Some(true),
        "a child under a remote root is always Remote"
    );
    let rows = gates.subtree_rows(&c, "notes", "n1").unwrap();
    assert!(
        rows.contains(&("notes".to_string(), "n1".to_string()))
            && rows.contains(&("note_photos".to_string(), "p1".to_string())),
        "remote-root subtrees include the root and descendants"
    );
}

#[test]
fn ancestor_keep_resolution_surfaces_step_errors() {
    let temp = tempfile::tempdir().expect("temp dir");
    let path = temp.path().join("gate-step-error.db");
    let c = Connection::open(&path).expect("open gate db");
    c.busy_timeout(std::time::Duration::from_millis(1))
        .expect("busy timeout");
    c.execute_test_sql(
        "CREATE TABLE albums (
                id TEXT PRIMARY KEY,
                _updated_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE releases (
                id TEXT PRIMARY KEY,
                album_id TEXT NOT NULL,
                managed INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE
            ) STRICT;
            INSERT INTO albums (id, _updated_at)
            VALUES ('a1', '0000000001000-0000-dev1');
            INSERT INTO releases (id, album_id, managed, _updated_at)
            VALUES ('r1', 'a1', 1, '0000000001000-0000-dev1');",
    );
    let tables = vec![
        SyncedTable::new(
            "releases",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("managed"),
        SyncedTable::new(
            "albums",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
    ];
    let gates = Gates::from_tables(&c, &tables).expect("gates");

    let blocker = Connection::open(&path).expect("open blocker");
    blocker
        .execute_batch(
            "BEGIN EXCLUSIVE; UPDATE albums SET _updated_at = _updated_at WHERE id = 'a1';",
        )
        .expect("hold exclusive write lock");

    let err = gates
        .root_kept_of(&c, "albums", "a1")
        .expect_err("locked step must surface as a gate error");
    assert!(
        matches!(err, GateError::Sql(_, _)),
        "unexpected error: {err}"
    );
}

#[test]
fn empty_clone_surfaces_detach_failure() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                _updated_at TEXT NOT NULL
            ) STRICT;",
    );
    let gates = Gates::from_tables(
        &c,
        &[SyncedTable::new(
            "notes",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .remote_root()],
    )
    .expect("gates");

    let err = with_empty_clone(&c, &gates, |alias, _tables| {
        c.execute_batch(&format!(
            "BEGIN;
                 INSERT INTO {alias}.notes (id, _updated_at)
                 VALUES ('n1', '0000000001000-0000-dev1');"
        ))
        .map_err(|e| GateError::Sql("write clone".to_string(), e))?;
        Ok(())
    })
    .expect_err("detach failure must surface");
    assert!(
        matches!(err, GateError::Sql(_, _)),
        "unexpected error: {err}"
    );
    c.execute_batch("ROLLBACK; DETACH DATABASE coven_gate_empty")
        .expect("cleanup clone");
}

#[test]
fn flip_false_to_true_reemits_full_subtree() {
    let c = conn();
    create_synced_schema(&c);
    let tables = test_synced_tables();

    // Cycle 1: create a private note with children. All cut.
    let out1 = capture_and_gate(
        &c,
        &tables,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', 'b', 0, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    assert!(
        walk(&out1).expect("walk").is_empty(),
        "cycle 1 emits nothing while private"
    );

    // Cycle 2: flip the gate true. Only the note UPDATE is captured; the
    // children are re-emitted by the flip logic.
    let out2 = capture_and_gate(
        &c,
        &tables,
        &["UPDATE notes SET shared = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
    );
    let c2 = walk(&out2).expect("walk");
    assert!(has_row(&c2, "notes", "n1"), "promoted note is emitted");
    assert!(
        has_row(&c2, "note_tags", "t1"),
        "re-emit includes the pre-existing tag child"
    );
    assert!(
        has_row(&c2, "note_photos", "p1"),
        "re-emit includes the pre-existing photo child"
    );

    // Apply cycle 2's output to a fresh peer: complete consistent subtree.
    let peer = conn();
    create_synced_schema(&peer);
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'"),
        "peer has the note"
    );
    assert_eq!(
        peer.query_test_text("SELECT title FROM notes WHERE id = 'n1'"),
        "Private"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM note_tags WHERE id = 't1'"),
        "peer has the tag"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM note_photos WHERE id = 'p1'"),
        "peer has the photo"
    );
}

#[test]
fn self_referencing_table_reemits_from_empty_clone() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE nodes (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES \"nodes\" (id)
            ) STRICT;",
    );
    let tables = vec![SyncedTable::new(
        "nodes",
        crate::protocol::synced_schema::RowIdentity::SharedKey,
    )
    .gated_by("shared")];

    let _ = capture_and_gate(
        &c,
        &tables,
        &["INSERT INTO nodes (id, parent_id, shared, _updated_at)
              VALUES ('root', NULL, 0, '0000000001000-0000-dev1')"],
    );

    let out = capture_and_gate(
        &c,
        &tables,
        &[
            "UPDATE nodes SET shared = 1, _updated_at = '0000000002000-0000-dev1'
              WHERE id = 'root'",
        ],
    );
    let changes = walk(&out).expect("walk");
    assert!(
        has_row(&changes, "nodes", "root"),
        "the promoted self-referencing root is emitted"
    );
}

#[test]
fn post_promotion_edit_is_single_update_not_reemit() {
    let c = conn();
    create_synced_schema(&c);
    let tables = test_synced_tables();

    // Create + promote in cycle 1 (note shared=1 immediately).
    let _ = capture_and_gate(
        &c,
        &tables,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );

    // Cycle 2: an ordinary edit to the already-shared note. Not a flip, so it
    // must emit exactly one UPDATE for the note and nothing else.
    let out = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET title = 'Renamed', _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
    let changes = walk(&out).expect("walk");
    assert_eq!(changes.len(), 1, "exactly one change");
    assert_eq!(changes[0].table, "notes");
    assert_eq!(changes[0].op, ChangeOp::Update);
    assert_eq!(changes[0].pk(), Some("n1"));
}

#[test]
fn multi_hop_fk_inheritance() {
    // grandchild -> child -> root(gated). The gate must flow two hops.
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE) STRICT",
    );
    let tables = vec![
        SyncedTable::new(
            "albums",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "comments",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];

    // Private album with a 2-level subtree: all cut.
    let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO albums (id, shared, _updated_at) VALUES ('a1', 0, '0000000001000-0000-dev1')",
                "INSERT INTO photos (id, album_id, _updated_at) VALUES ('p1', 'a1', '0000000001000-0000-dev1')",
                "INSERT INTO comments (id, photo_id, _updated_at) VALUES ('c1', 'p1', '0000000001000-0000-dev1')",
            ],
        );
    assert!(
        walk(&out).expect("walk").is_empty(),
        "private 2-level subtree fully cut"
    );

    // Flip the album true: re-emit must reach the grandchild comment.
    let out = capture_and_gate(
        &c,
        &tables,
        &["UPDATE albums SET shared = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'a1'"],
    );
    let changes = walk(&out).expect("walk");
    assert!(has_row(&changes, "albums", "a1"));
    assert!(
        has_row(&changes, "photos", "p1"),
        "one-hop child re-emitted"
    );
    assert!(
        has_row(&changes, "comments", "c1"),
        "two-hop grandchild re-emitted"
    );
}

#[test]
fn delete_gated_false_strips_private_subtrees_in_place() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE albums (id TEXT PRIMARY KEY, shared INTEGER NOT NULL DEFAULT 0, \
             _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE photos (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE comments (id TEXT PRIMARY KEY, photo_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (photo_id) REFERENCES photos (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE settings (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT",
    );
    let tables = vec![
        SyncedTable::new(
            "albums",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "photos",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "comments",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "settings",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];

    c.execute_test_sql("INSERT INTO albums (id, shared, _updated_at) VALUES ('priv', 0, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO photos (id, album_id, _updated_at) VALUES ('priv_p', 'priv', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO comments (id, photo_id, _updated_at) VALUES ('priv_c', 'priv_p', '0000000001000-0000-dev1')");
    c.execute_test_sql(
        "INSERT INTO albums (id, shared, _updated_at) VALUES ('pub', 1, '0000000001000-0000-dev1')",
    );
    c.execute_test_sql("INSERT INTO photos (id, album_id, _updated_at) VALUES ('pub_p', 'pub', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO comments (id, photo_id, _updated_at) VALUES ('pub_c', 'pub_p', '0000000001000-0000-dev1')");
    c.execute_test_sql(
        "INSERT INTO settings (id, _updated_at) VALUES ('s1', '0000000001000-0000-dev1')",
    );

    let gates = Gates::from_tables(&c, &tables).expect("gates");
    gates.delete_gated_false(&c).expect("delete gated-false");

    assert!(!c.test_row_exists("SELECT 1 FROM albums WHERE id = 'priv'"));
    assert!(!c.test_row_exists("SELECT 1 FROM photos WHERE id = 'priv_p'"));
    assert!(!c.test_row_exists("SELECT 1 FROM comments WHERE id = 'priv_c'"));
    assert!(c.test_row_exists("SELECT 1 FROM albums WHERE id = 'pub'"));
    assert!(c.test_row_exists("SELECT 1 FROM photos WHERE id = 'pub_p'"));
    assert!(c.test_row_exists("SELECT 1 FROM comments WHERE id = 'pub_c'"));
    assert!(c.test_row_exists("SELECT 1 FROM settings WHERE id = 's1'"));
}

// ---- upward gate (gated_by_descendants) ----------------------------------

fn album_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "releases",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("managed"),
        SyncedTable::new(
            "albums",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "artists",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "album_artists",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "tracks",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ]
}

fn create_album_schema(c: &Connection) {
    c.execute_test_sql(
        "CREATE TABLE artists (id TEXT PRIMARY KEY, name TEXT, _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE albums (id TEXT PRIMARY KEY, artist_id TEXT, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id)) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE album_artists (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             artist_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE, \
             FOREIGN KEY (artist_id) REFERENCES artists (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE releases (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             managed INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE tracks (id TEXT PRIMARY KEY, release_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (release_id) REFERENCES releases (id) ON DELETE CASCADE) STRICT",
    );
}

/// The inferred keep-children of `tbl`, as `(child, fk column name)`, sorted.
fn inferred_children(gates: &Gates, tbl: &str) -> Vec<(String, String)> {
    match gates.tables.get(tbl) {
        Some(TableGate::Parent { children }) => {
            let mut out: Vec<(String, String)> = children
                .iter()
                .map(|(ch, col, _)| (ch.clone(), col.name.clone()))
                .collect();
            out.sort();
            out
        }
        Some(_) => panic!("{tbl} is in the gate map but not modeled as a Parent"),
        None => panic!("{tbl} is absent from the gate map; expected a Parent"),
    }
}

/// The downward gate-parent `from_tables` chose for `tbl`, as `(parent, fk
/// column name)`. Panics if `tbl` is not modeled as an inheriting `Child`.
fn downward_parent(gates: &Gates, tbl: &str) -> (String, String) {
    match gates.tables.get(tbl) {
        Some(TableGate::Child { fk_col, parent, .. }) => (parent.clone(), fk_col.name.clone()),
        other => panic!(
            "{tbl} must be an inheriting Child, got present={}",
            other.is_some()
        ),
    }
}

#[test]
fn inference_resolves_children_and_join_parent() {
    let c = conn();
    create_album_schema(&c);
    let gates = Gates::from_tables(&c, &album_tables()).expect("gates");

    assert_eq!(
        inferred_children(&gates, "albums"),
        vec![("releases".to_string(), "album_id".to_string())],
        "albums is kept only by releases (the album_artists back-edge is excluded)"
    );
    assert_eq!(
        inferred_children(&gates, "artists"),
        vec![
            ("album_artists".to_string(), "artist_id".to_string()),
            ("albums".to_string(), "artist_id".to_string()),
        ],
        "artists is kept by albums OR album_artists"
    );
    assert_eq!(
        downward_parent(&gates, "album_artists"),
        ("albums".to_string(), "album_id".to_string()),
    );
}

#[test]
fn downward_parent_tie_uses_complete_foreign_key_shape() {
    fn selected_child_column(foreign_keys: &str) -> String {
        let c = conn();
        c.execute_test_sql(
            "CREATE TABLE roots (
                    id TEXT PRIMARY KEY,
                    shared INTEGER NOT NULL,
                    _updated_at TEXT NOT NULL
                ) STRICT;",
        );
        c.execute_test_sql(&format!(
            "CREATE TABLE children (
                        id TEXT PRIMARY KEY,
                        a_root_id TEXT NOT NULL,
                        z_root_id TEXT NOT NULL,
                        _updated_at TEXT NOT NULL,
                        {foreign_keys}
                    ) STRICT;"
        ));
        let tables = vec![
            SyncedTable::new(
                "roots",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .gated_by("shared"),
            SyncedTable::new(
                "children",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            ),
        ];
        downward_parent(&Gates::from_tables(&c, &tables).expect("gates"), "children").1
    }

    let a_then_z = selected_child_column(
        "FOREIGN KEY (a_root_id) REFERENCES roots (id),
             FOREIGN KEY (z_root_id) REFERENCES roots (id)",
    );
    let z_then_a = selected_child_column(
        "FOREIGN KEY (z_root_id) REFERENCES roots (id),
             FOREIGN KEY (a_root_id) REFERENCES roots (id)",
    );

    assert_eq!(a_then_z, "a_root_id");
    assert_eq!(z_then_a, a_then_z);
}

#[test]
fn downward_parent_is_most_specific_not_lexicographic() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE aouter (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE zinner (id TEXT PRIMARY KEY, aouter_id TEXT, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (aouter_id) REFERENCES aouter (id)) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE zgated (id TEXT PRIMARY KEY, zinner_id TEXT NOT NULL, \
             shared INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (zinner_id) REFERENCES zinner (id)) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE joiner (id TEXT PRIMARY KEY, aouter_id TEXT NOT NULL, \
             zinner_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (aouter_id) REFERENCES aouter (id), \
             FOREIGN KEY (zinner_id) REFERENCES zinner (id)) STRICT",
    );
    let tables = vec![
        SyncedTable::new(
            "aouter",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "zinner",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "zgated",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("shared"),
        SyncedTable::new(
            "joiner",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
    ];
    let gates = Gates::from_tables(&c, &tables).expect("gates");
    assert_eq!(
        downward_parent(&gates, "joiner"),
        ("zinner".to_string(), "zinner_id".to_string()),
        "the most-specific (deeper) ancestor wins even though it sorts \
             lexicographically later than `aouter`"
    );
}

#[test]
fn fk_topological_order_is_parent_first() {
    let c = conn();
    create_album_schema(&c);
    let gates = Gates::from_tables(&c, &album_tables()).expect("gates");
    let order = gates.gated_tables_parent_first(&c).expect("topo order");
    let pos = |t: &str| order.iter().position(|x| *x == t).unwrap();
    assert!(pos("artists") < pos("albums"), "artist before album");
    assert!(pos("albums") < pos("releases"), "album before release");
    assert!(pos("releases") < pos("tracks"), "release before track");
    assert!(
        pos("albums") < pos("album_artists"),
        "album before album_artists"
    );
    assert!(
        pos("artists") < pos("album_artists"),
        "artist before album_artists"
    );
}

#[test]
fn delete_gated_false_prunes_empty_ancestors() {
    let c = conn();
    create_album_schema(&c);

    c.execute_test_sql(
        "INSERT INTO artists (id, _updated_at) VALUES ('A1', '0000000001000-0000-dev1')",
    );
    c.execute_test_sql(
        "INSERT INTO artists (id, _updated_at) VALUES ('A2', '0000000001000-0000-dev1')",
    );
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL_MIXED', 'A2', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_EMPTY', 'AL_EMPTY', 'A1', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA_MIXED', 'AL_MIXED', 'A2', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN', 'AL_EMPTY', 0, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_MAN', 'AL_MIXED', 1, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R_UNMAN2', 'AL_MIXED', 0, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_UNMAN', 'R_UNMAN', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T_MAN', 'R_MAN', '0000000001000-0000-dev1')");

    let gates = Gates::from_tables(&c, &album_tables()).expect("gates");
    gates.delete_gated_false(&c).expect("delete gated-false");

    assert!(
        !c.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL_EMPTY'"),
        "empty album pruned"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM releases WHERE id = 'R_UNMAN'"),
        "unmanaged release gone"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T_UNMAN'"),
        "track of unmanaged release gone"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA_EMPTY'"),
        "album_artists of pruned album gone"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM artists WHERE id = 'A1'"),
        "artist with no kept album pruned"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL_MIXED'"),
        "mixed album survives"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM releases WHERE id = 'R_MAN'"),
        "managed release survives"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T_MAN'"),
        "track of managed release survives"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM releases WHERE id = 'R_UNMAN2'"),
        "the unmanaged sibling release is still cut"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA_MIXED'"),
        "album_artists of surviving album kept"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM artists WHERE id = 'A2'"),
        "artist kept via a surviving album"
    );
}

#[test]
fn changeset_cut_drops_orphan_ancestor() {
    let c = conn();
    create_album_schema(&c);
    let out = capture_and_gate(
            &c,
            &album_tables(),
            &[
                "INSERT INTO albums (id, _updated_at) VALUES ('AL', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')",
            ],
        );
    let changes = walk(&out).expect("walk");
    assert!(
        !has_row(&changes, "albums", "AL"),
        "orphan album cut (no kept release)"
    );
    assert!(!has_row(&changes, "releases", "R"), "unmanaged release cut");
    assert!(
        !has_row(&changes, "tracks", "T"),
        "track of unmanaged release cut"
    );
}

#[test]
fn deleting_a_shared_album_with_its_release_propagates_the_album_delete() {
    let c = conn();
    create_album_schema(&c);

    // A shared album: a managed release under it makes the album sync to peers.
    c.execute_test_sql("INSERT INTO artists (id, name, _updated_at) VALUES ('ar1', 'Artist', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('al1', 'ar1', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO tracks (id, release_id, _updated_at) VALUES ('tr1', 're1', '0000000001000-0000-dev1')");

    // Deleting the album cascades the release and track; the capture records the
    // album DELETE plus the cascaded child DELETEs.
    let out = capture_and_gate(
        &c,
        &album_tables(),
        &["DELETE FROM albums WHERE id = 'al1'"],
    );
    let changes = walk(&out).expect("walk");

    assert!(
        has_row(&changes, "releases", "re1"),
        "the managed release's removal propagates"
    );
    assert!(
        has_row(&changes, "albums", "al1"),
        "the album's own removal must propagate so a peer drops the now-empty \
             album instead of keeping a phantom"
    );
    assert!(
        has_row(&changes, "tracks", "tr1"),
        "the track's removal must propagate too — apply does not cascade, so a \
             cut track would orphan under the removed release on every peer"
    );
}

#[test]
fn deleting_one_release_keeps_the_surviving_shared_album() {
    let c = conn();
    create_album_schema(&c);

    // An album with two managed releases; deleting one leaves the album shared.
    c.execute_test_sql(
        "INSERT INTO albums (id, _updated_at) VALUES ('al1', '0000000001000-0000-dev1')",
    );
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re2', 'al1', 1, '0000000001000-0000-dev1')");

    let out = capture_and_gate(
        &c,
        &album_tables(),
        &["DELETE FROM releases WHERE id = 're1'"],
    );
    let changes = walk(&out).expect("walk");

    assert!(
        has_row(&changes, "releases", "re1"),
        "the deleted managed release's removal propagates"
    );
    assert!(
        !has_row(&changes, "albums", "al1"),
        "the album survives (it still has a managed release), so its row is not \
             emitted as a deletion"
    );
}

#[test]
fn deleting_a_private_album_does_not_propagate_its_delete() {
    let c = conn();
    create_album_schema(&c);

    // A private album: only an unmanaged release, so it never synced to peers.
    c.execute_test_sql(
        "INSERT INTO albums (id, _updated_at) VALUES ('al1', '0000000001000-0000-dev1')",
    );
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 0, '0000000001000-0000-dev1')");

    let out = capture_and_gate(
        &c,
        &album_tables(),
        &["DELETE FROM albums WHERE id = 'al1'"],
    );
    let changes = walk(&out).expect("walk");

    assert!(
        !has_row(&changes, "releases", "re1"),
        "the unmanaged release was never shared; its removal is cut"
    );
    assert!(
        !has_row(&changes, "albums", "al1"),
        "a never-shared album's removal must not propagate — its DELETE carries \
             old column values a peer should never receive"
    );
}

#[test]
fn deleting_a_shared_artist_with_its_album_propagates_the_artist_delete() {
    let c = conn();
    create_album_schema(&c);

    c.execute_test_sql("INSERT INTO artists (id, name, _updated_at) VALUES ('ar1', 'Artist', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('al1', 'ar1', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('aa1', 'al1', 'ar1', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('re1', 'al1', 1, '0000000001000-0000-dev1')");

    // album.artist_id has no ON DELETE CASCADE, so an artist is removed by
    // deleting its albums (cascading releases/album_artists) then the artist.
    let out = capture_and_gate(
        &c,
        &album_tables(),
        &[
            "DELETE FROM albums WHERE id = 'al1'",
            "DELETE FROM artists WHERE id = 'ar1'",
        ],
    );
    let changes = walk(&out).expect("walk");

    assert!(
        has_row(&changes, "albums", "al1"),
        "the shared album's removal propagates"
    );
    assert!(
        has_row(&changes, "artists", "ar1"),
        "the artist's removal must propagate up the chain once its last kept \
             album is being removed"
    );
}

#[test]
fn flip_reemits_whole_connected_component_to_peer() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // Cycle 1: build the private graph. Nothing should escape.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1')",
            ],
        );
    assert!(
        walk(&out1).expect("walk").is_empty(),
        "private graph emits nothing"
    );

    // Cycle 2: flip the release managed. Re-emit the whole component.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'"],
        );
    let changes = walk(&out2).expect("walk");
    assert!(
        has_row(&changes, "releases", "R"),
        "promoted release emitted"
    );
    assert!(
        has_row(&changes, "albums", "AL"),
        "ancestor album re-emitted"
    );
    assert!(
        has_row(&changes, "artists", "AR"),
        "ancestor artist re-emitted"
    );
    assert!(
        has_row(&changes, "tracks", "T"),
        "descendant track re-emitted"
    );
    assert!(
        has_row(&changes, "album_artists", "AA"),
        "kept child of ancestor re-emitted"
    );

    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR'"),
        "peer has artist"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "peer has album"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA'"),
        "peer has album_artists"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R'"),
        "peer has release"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T'"),
        "peer has track"
    );
}

#[test]
fn flip_reverted_before_gate_emits_no_insert() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // A private graph: artist, album, and an unmanaged release. Nothing has
    // ever reached a peer.
    c.execute_test_sql("INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R', 'AL', 0, '0000000001000-0000-dev1')");

    // The journal batch captures the release flipping managed false→true.
    let bytes = capture(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'"],
        );

    // A host write lands between the batch load and the gate run, flipping the
    // gate back false. The live db now reads managed=0 for R, so nothing in its
    // component is currently kept.
    c.execute_test_sql(
        "UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R'",
    );

    let gates = Gates::from_tables(&c, &tables).expect("build gates");
    let out = gate_outbound(&c, &bytes, &gates).expect("gate outbound");
    let changes = walk(&out).expect("walk");

    assert!(
        !has_row(&changes, "releases", "R"),
        "a root re-flipped false before the gate runs must not leak its private \
             row values as an INSERT"
    );
    assert!(
        !has_row(&changes, "albums", "AL"),
        "the now-childless ancestor album must not be re-emitted"
    );
    assert!(
        !has_row(&changes, "artists", "AR"),
        "the now-childless ancestor artist must not be re-emitted"
    );
}

#[test]
fn reparent_onto_a_cut_ancestor_reemits_it_to_peer() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // Cycle 1: an artist, two albums under it, and a managed release under
    // AL1. AL2 has no managed release, so the gate cuts it — the peer never
    // receives it.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL1', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL2', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL1', 1, '0000000001000-0000-dev1')",
            ],
        );
    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out1, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL1'"),
        "peer has the shared album AL1"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL2'"),
        "AL2 was cut (no managed release) — the peer never received it"
    );

    // Cycle 2: reparent the managed release onto AL2 (previously cut). The
    // gate must re-emit AL2 (and its component), or the peer applies the bare
    // FK change against a missing album and is left with a dangling release.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET album_id = 'AL2', _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    let changes = walk(&out2).expect("walk");
    assert!(
        has_row(&changes, "albums", "AL2"),
        "the newly-referenced album AL2 must be re-emitted"
    );

    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL2'"),
        "peer now has AL2"
    );
    assert_eq!(
        peer.query_test_text("SELECT album_id FROM releases WHERE id = 'R1'"),
        "AL2",
        "R1 now points at AL2 on the peer"
    );
    assert!(
        !peer.test_row_exists(
            "SELECT 1 FROM releases r WHERE NOT EXISTS \
                 (SELECT 1 FROM albums a WHERE a.id = r.album_id)"
        ),
        "no release on the peer points at a missing album"
    );
}

#[test]
fn flip_reemits_sideways_featured_artist() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // AR1 owns AL1; AR2 is featured via AA; release R1 unmanaged.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR1', 'Owner', '0000000001000-0000-dev1')",
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR2', 'Featured', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL1', 'AR1', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL1', 'AR2', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL1', 0, '0000000001000-0000-dev1')",
            ],
        );
    assert!(
        walk(&out1).expect("walk").is_empty(),
        "private graph emits nothing"
    );

    // Flip R1 managed.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    let changes = walk(&out2).expect("walk");
    assert!(
        has_row(&changes, "album_artists", "AA"),
        "featured join row re-emitted"
    );
    assert!(
        has_row(&changes, "artists", "AR2"),
        "featured artist re-emitted"
    );

    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA'"),
        "peer has join row"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR2'"),
        "peer has featured artist"
    );
}

#[test]
fn second_flip_is_idempotent_under_lww() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // Cycle 1: an album with one managed release, synced to the peer.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
            ],
        );

    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out1, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "peer has the album after cycle 1"
    );

    // Cycle 2a: insert a second release unmanaged (stays private, cut).
    let _ = capture_and_gate(
            &c,
            &tables,
            &["INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R2', 'AL', 0, '0000000002000-0000-dev1')"],
        );

    // Cycle 2b: flip the second release managed. Re-emit re-sends the album.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R2'"],
        );
    let changes = walk(&out2).expect("walk");
    assert!(
        has_row(&changes, "albums", "AL"),
        "album re-emitted on the second flip"
    );
    assert!(
        has_row(&changes, "releases", "R2"),
        "second release emitted"
    );

    // Applying the duplicate album INSERT must not error; peer consistent.
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "album still present"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "first release still present"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R2'"),
        "second release now present"
    );
    assert_eq!(
        query_int(&peer, "SELECT COUNT(*) FROM albums WHERE id = 'AL'"),
        1,
        "the duplicate INSERT did not create a second album row"
    );
}

// ---- retract (gate true→false) -------------------------------------------

fn has_op(
    changes: &[coven_foundation::changeset::RowChange],
    table: &str,
    pk: &str,
    op: ChangeOp,
) -> bool {
    changes
        .iter()
        .any(|c| c.table == table && c.pk() == Some(pk) && c.op == op)
}

#[test]
fn retract_emits_deletes_and_local_rows_remain() {
    let c = conn();
    create_synced_schema(&c);
    let tables = test_synced_tables();

    // Cycle 1: a shared note with children (inserted shared=1 → flip emits the
    // subtree as INSERTs).
    let _ = capture_and_gate(
        &c,
        &tables,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );

    // Cycle 2: flip the gate false. The captured UPDATE-to-false is skipped;
    // the gate synthesizes DELETEs for the root and its now-private subtree.
    let out = capture_and_gate(
        &c,
        &tables,
        &["UPDATE notes SET shared = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
    );
    let changes = walk(&out).expect("walk");
    assert!(
        has_op(&changes, "notes", "n1", ChangeOp::Delete),
        "the retracted root is emitted as a DELETE"
    );
    assert!(
        has_op(&changes, "note_tags", "t1", ChangeOp::Delete),
        "the tag child leaves the shared set as a DELETE"
    );
    assert!(
        has_op(&changes, "note_photos", "p1", ChangeOp::Delete),
        "the photo child leaves the shared set as a DELETE"
    );
    assert!(
        changes.iter().all(|c| c.op == ChangeOp::Delete),
        "retract emits only DELETEs (verifies the reverse session_diff direction)"
    );

    // The local rows stay — retract is peer-only, never a local DELETE FROM.
    assert!(c.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'"));
    assert!(c.test_row_exists("SELECT 1 FROM note_tags WHERE id = 't1'"));
    assert!(c.test_row_exists("SELECT 1 FROM note_photos WHERE id = 'p1'"));
}

#[test]
fn peer_applies_retract_and_subtree_is_removed() {
    let c = conn();
    create_synced_schema(&c);
    let tables = test_synced_tables();

    // Share the subtree to a peer.
    let out1 = capture_and_gate(
        &c,
        &tables,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Public', 'b', 1, '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
                 VALUES ('t1', 'n1', 'green', '0000000001000-0000-dev1', '2026-01-01')",
            "INSERT INTO note_photos (id, note_id, kind, _updated_at, created_at) \
                 VALUES ('p1', 'n1', 'cover', '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    let peer = conn();
    create_synced_schema(&peer);
    peer.apply_test_changeset(&out1, &tables);
    assert!(peer.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'"));
    assert!(peer.test_row_exists("SELECT 1 FROM note_tags WHERE id = 't1'"));
    assert!(peer.test_row_exists("SELECT 1 FROM note_photos WHERE id = 'p1'"));

    // Retract and apply: the whole subtree is gone on the peer.
    let out2 = capture_and_gate(
        &c,
        &tables,
        &["UPDATE notes SET shared = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
    );
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        !peer.test_row_exists("SELECT 1 FROM notes WHERE id = 'n1'"),
        "peer drops the retracted root"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM note_tags WHERE id = 't1'"),
        "peer drops the tag child"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM note_photos WHERE id = 'p1'"),
        "peer drops the photo child"
    );
}

#[test]
fn retract_one_of_two_managed_roots_spares_sibling_and_ancestor() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // Two managed releases under one album/artist, with a track each.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R2', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T2', 'R2', '0000000001000-0000-dev1')",
            ],
        );
    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out1, &tables);

    // Retract only R1.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    let changes = walk(&out2).expect("walk");
    assert!(
        has_op(&changes, "releases", "R1", ChangeOp::Delete),
        "R1 deleted"
    );
    assert!(
        has_op(&changes, "tracks", "T1", ChangeOp::Delete),
        "T1 deleted"
    );
    assert!(
        !has_row(&changes, "releases", "R2"),
        "the sibling managed release is spared"
    );
    assert!(
        !has_row(&changes, "tracks", "T2"),
        "the sibling's track is spared"
    );
    assert!(
        !has_row(&changes, "albums", "AL"),
        "the album is spared (still has a managed release)"
    );
    assert!(!has_row(&changes, "artists", "AR"), "the artist is spared");
    assert!(
        !has_row(&changes, "album_artists", "AA"),
        "the join row is spared"
    );

    peer.apply_test_changeset(&out2, &tables);
    assert!(
        !peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "R1 gone on peer"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"),
        "T1 gone on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R2'"),
        "R2 kept on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T2'"),
        "T2 kept on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "album kept on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR'"),
        "artist kept on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA'"),
        "join row kept on peer"
    );
}

#[test]
fn retract_last_root_under_ancestor_deletes_ancestor() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // A single managed release under an album/artist.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
            ],
        );
    let peer = conn();
    create_album_schema(&peer);
    peer.apply_test_changeset(&out1, &tables);

    // Retract the last managed release: the now-childless album AND artist are
    // deleted too.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    let changes = walk(&out2).expect("walk");
    for (table, pk) in [
        ("releases", "R1"),
        ("tracks", "T1"),
        ("albums", "AL"),
        ("artists", "AR"),
        ("album_artists", "AA"),
    ] {
        assert!(
            has_op(&changes, table, pk, ChangeOp::Delete),
            "{table}.{pk} is deleted when the last managed root is retracted"
        );
    }

    peer.apply_test_changeset(&out2, &tables);
    assert!(!peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"));
    assert!(!peer.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"));
    assert!(
        !peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "the now-childless album is deleted on the peer"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR'"),
        "the now-childless artist is deleted on the peer"
    );
    assert!(!peer.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA'"));
}

#[test]
fn gated_false_root_from_start_emits_no_deletes() {
    let c = conn();
    create_synced_schema(&c);
    let tables = test_synced_tables();

    // Cycle 1: a private note (never shared). Nothing emitted.
    let out1 = capture_and_gate(
        &c,
        &tables,
        &[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Private', 'b', 0, '0000000001000-0000-dev1', '2026-01-01')",
        ],
    );
    assert!(
        walk(&out1).expect("walk").is_empty(),
        "a gated-false root inserted private emits nothing — no DELETE"
    );

    // Cycle 2: edit the still-private note (no gate transition). The update is
    // cut as before; no retract fires (there was never a true→false flip).
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE notes SET body = 'edited', _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'"],
        );
    assert!(
            walk(&out2).expect("walk").is_empty(),
            "editing a never-shared gated-false root emits nothing — retract only fires on a true→false transition"
        );
}

#[test]
fn reshare_after_retract_reemits_inserts() {
    let c = conn();
    create_album_schema(&c);
    let tables = album_tables();

    // Cycle 1: a private subtree (release unmanaged). Nothing escapes.
    let _ = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1')",
            ],
        );
    let peer = conn();
    create_album_schema(&peer);

    // Cycle 2: share (false→true). Peer gets the whole component.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "shared to peer"
    );

    // Cycle 3: retract (true→false). Peer loses the component.
    let out3 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R1'"],
        );
    peer.apply_test_changeset(&out3, &tables);
    assert!(
        !peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "retracted from peer"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "album retracted"
    );

    // Cycle 4: re-share (false→true) re-emits full INSERTs — round-trip.
    let out4 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000004000-0000-dev1' WHERE id = 'R1'"],
        );
    let changes = walk(&out4).expect("walk");
    for (table, pk) in [
        ("releases", "R1"),
        ("tracks", "T1"),
        ("albums", "AL"),
        ("artists", "AR"),
        ("album_artists", "AA"),
    ] {
        assert!(
            has_op(&changes, table, pk, ChangeOp::Insert),
            "{table}.{pk} re-emitted as an INSERT on re-share"
        );
    }
    peer.apply_test_changeset(&out4, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "re-shared to peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"),
        "track back on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "album back on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR'"),
        "artist back on peer"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM album_artists WHERE id = 'AA'"),
        "join row back"
    );
}

#[test]
fn multi_device_share_then_retract() {
    // Device A shares a subtree (false→true), device B applies it; A retracts
    // (true→false), B applies and the subtree is gone on B.
    let a = conn();
    create_album_schema(&a);
    let tables = album_tables();
    let b = conn();
    create_album_schema(&b);

    // A builds a private subtree, then flips it shared.
    let _ = capture_and_gate(
            &a,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist', '0000000001000-0000-devA')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-devA')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-devA')",
                "INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-devA')",
            ],
        );
    let share = capture_and_gate(
            &a,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-devA' WHERE id = 'R1'"],
        );
    b.apply_test_changeset(&share, &tables);
    assert!(
        b.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "B has the release"
    );
    assert!(
        b.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"),
        "B has the track"
    );
    assert!(
        b.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "B has the album"
    );

    // A retracts; B applies; the subtree is gone on B while A keeps it locally.
    let retract = capture_and_gate(
            &a,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-devA' WHERE id = 'R1'"],
        );
    b.apply_test_changeset(&retract, &tables);
    assert!(
        !b.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "B drops the release"
    );
    assert!(
        !b.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"),
        "B drops the track"
    );
    assert!(
        !b.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "B drops the album"
    );

    // A keeps the rows locally — retract is peer-only.
    assert!(
        a.test_row_exists("SELECT 1 FROM releases WHERE id = 'R1'"),
        "A keeps the release"
    );
    assert!(
        a.test_row_exists("SELECT 1 FROM tracks WHERE id = 'T1'"),
        "A keeps the track"
    );
    assert!(
        a.test_row_exists("SELECT 1 FROM albums WHERE id = 'AL'"),
        "A keeps the album"
    );
}

// ---- assets ride the gate, never grant keep ------------------------------

/// The album set plus two assets: a cover (child of the `releases` root) and
/// an artist image (child of the `artists` ancestor). Each is host-provided
/// decoration that rides its subject's gate but never keeps the subject alive.
fn album_asset_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new(
            "releases",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by("managed"),
        SyncedTable::new(
            "albums",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "artists",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .gated_by_descendants(),
        SyncedTable::new(
            "album_artists",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "tracks",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        ),
        SyncedTable::new(
            "covers",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .asset(),
        SyncedTable::new(
            "artist_images",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        )
        .asset(),
    ]
}

fn create_album_asset_schema(c: &Connection) {
    create_album_schema(c);
    c.execute_test_sql(
        "CREATE TABLE covers (id TEXT PRIMARY KEY, release_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (release_id) REFERENCES releases (id) ON DELETE CASCADE) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE artist_images (id TEXT PRIMARY KEY, artist_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id) ON DELETE CASCADE) STRICT",
    );
}

/// A `managed` release keeps its artist alive, and the artist image rides the
/// kept artist's gate to peers — same for the cover under the release.
#[test]
fn remote_release_keeps_artist_and_its_image() {
    let c = conn();
    create_album_asset_schema(&c);
    let tables = album_asset_tables();

    let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist Name', '0000000001000-0000-dev1')",
                "INSERT INTO artist_images (id, artist_id, _updated_at) VALUES ('AI', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')",
                "INSERT INTO covers (id, release_id, _updated_at) VALUES ('CV', 'R1', '0000000001000-0000-dev1')",
            ],
        );
    let changes = walk(&out).expect("walk");
    assert!(
        has_row(&changes, "artists", "AR"),
        "the artist is kept by its remote release"
    );
    assert!(
        has_row(&changes, "artist_images", "AI"),
        "the artist image rides the kept artist's gate"
    );
    assert!(
        has_row(&changes, "releases", "R1"),
        "the remote release syncs"
    );
    assert!(
        has_row(&changes, "covers", "CV"),
        "the cover rides the kept release's gate"
    );

    // The image and cover rows land on a fresh peer.
    let peer = conn();
    create_album_asset_schema(&peer);
    peer.apply_test_changeset(&out, &tables);
    assert!(peer.test_row_exists("SELECT 1 FROM artist_images WHERE id = 'AI'"));
    assert!(peer.test_row_exists("SELECT 1 FROM covers WHERE id = 'CV'"));
}

/// An artist whose only release is local (gated-false) is NOT kept — its image
/// does not keep it alive — so nothing about it leaks to peers.
#[test]
fn image_alone_without_a_remote_release_is_not_kept() {
    let c = conn();
    create_album_asset_schema(&c);
    let tables = album_asset_tables();

    let out = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist Name', '0000000001000-0000-dev1')",
                "INSERT INTO artist_images (id, artist_id, _updated_at) VALUES ('AI', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO covers (id, release_id, _updated_at) VALUES ('CV', 'R1', '0000000001000-0000-dev1')",
            ],
        );
    assert!(
            walk(&out).expect("walk").is_empty(),
            "an artist with only an image and a local release leaks nothing — the image is not a keep-reason"
        );
}

/// make-Remote (gate false→true) re-emits the artist and its image as INSERTs;
/// make-Local (true→false) retracts both as DELETEs. The asset tracks its
/// subject in both directions.
#[test]
fn make_remote_reemits_image_then_make_local_retracts_it() {
    let c = conn();
    create_album_asset_schema(&c);
    let tables = album_asset_tables();

    // A local release shares nothing.
    let out1 = capture_and_gate(
            &c,
            &tables,
            &[
                "INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist Name', '0000000001000-0000-dev1')",
                "INSERT INTO artist_images (id, artist_id, _updated_at) VALUES ('AI', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')",
                "INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 0, '0000000001000-0000-dev1')",
                "INSERT INTO covers (id, release_id, _updated_at) VALUES ('CV', 'R1', '0000000001000-0000-dev1')",
            ],
        );
    assert!(
        walk(&out1).expect("walk").is_empty(),
        "a local release shares nothing"
    );

    let peer = conn();
    create_album_asset_schema(&peer);

    // make-Remote: the artist + its image re-emit as INSERTs.
    let out2 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'"],
        );
    let c2 = walk(&out2).expect("walk");
    assert!(has_row(&c2, "artists", "AR"), "the artist is promoted");
    assert!(
        has_row(&c2, "artist_images", "AI"),
        "the image re-emits with its now-remote artist"
    );
    assert!(
        has_row(&c2, "covers", "CV"),
        "the cover re-emits with its now-remote release"
    );
    peer.apply_test_changeset(&out2, &tables);
    assert!(
        peer.test_row_exists("SELECT 1 FROM artist_images WHERE id = 'AI'"),
        "peer has the image"
    );
    assert!(
        peer.test_row_exists("SELECT 1 FROM covers WHERE id = 'CV'"),
        "peer has the cover"
    );

    // make-Local: the artist + image retract as DELETEs.
    let out3 = capture_and_gate(
            &c,
            &tables,
            &["UPDATE releases SET managed = 0, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R1'"],
        );
    let c3 = walk(&out3).expect("walk");
    assert!(
        has_op(&c3, "artists", "AR", ChangeOp::Delete),
        "the now-childless artist retracts"
    );
    assert!(
        has_op(&c3, "artist_images", "AI", ChangeOp::Delete),
        "the image retracts with its artist"
    );
    assert!(
        has_op(&c3, "covers", "CV", ChangeOp::Delete),
        "the cover retracts with its release"
    );
    peer.apply_test_changeset(&out3, &tables);
    assert!(
        !peer.test_row_exists("SELECT 1 FROM artist_images WHERE id = 'AI'"),
        "peer drops the image"
    );
    assert!(
        !peer.test_row_exists("SELECT 1 FROM covers WHERE id = 'CV'"),
        "peer drops the cover"
    );
}

/// Keep resolution terminates with an asset child present: building the gate
/// model and evaluating every keep-clause completes, because excluding the
/// asset from its subject's keep-children breaks the otherwise-circular
/// artist-kept-by-children vs. image-rides-artist relation.
#[test]
fn keep_resolution_terminates_with_an_asset_child() {
    let c = conn();
    create_album_asset_schema(&c);
    let tables = album_asset_tables();

    let gates =
        Gates::from_tables(&c, &tables).expect("gate model builds with an asset child present");
    assert!(
        !inferred_children(&gates, "artists")
            .iter()
            .any(|(child, _)| child == "artist_images"),
        "the artist image is never a keep-reason for its artist"
    );

    // A kept artist (one remote release + image) and an unkept one (image
    // only). `delete_gated_false` builds and evaluates every gated table's
    // keep-clause; that it returns proves keep resolution terminates.
    c.execute_test_sql("INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Artist Name', '0000000001000-0000-dev1')",
        );
    c.execute_test_sql("INSERT INTO artist_images (id, artist_id, _updated_at) VALUES ('AI', 'AR', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO albums (id, artist_id, _updated_at) VALUES ('AL', 'AR', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO album_artists (id, album_id, artist_id, _updated_at) VALUES ('AA', 'AL', 'AR', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO releases (id, album_id, managed, _updated_at) VALUES ('R1', 'AL', 1, '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO covers (id, release_id, _updated_at) VALUES ('CV', 'R1', '0000000001000-0000-dev1')");
    // A second artist kept by nothing but an image.
    c.execute_test_sql("INSERT INTO artists (id, name, _updated_at) VALUES ('AR2', 'Other Artist', '0000000001000-0000-dev1')");
    c.execute_test_sql("INSERT INTO artist_images (id, artist_id, _updated_at) VALUES ('AI2', 'AR2', '0000000001000-0000-dev1')");

    gates
        .delete_gated_false(&c)
        .expect("keep resolution terminates with an asset child");

    assert!(
        c.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR'"),
        "the artist with a remote release survives the prune"
    );
    assert!(
        c.test_row_exists("SELECT 1 FROM artist_images WHERE id = 'AI'"),
        "its image survives with it"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM artists WHERE id = 'AR2'"),
        "the image-only artist is pruned — the image did not keep it"
    );
    assert!(
        !c.test_row_exists("SELECT 1 FROM artist_images WHERE id = 'AI2'"),
        "the image-only artist's image is pruned with it"
    );
}

/// `.asset()` is load-bearing: in a topology where the join-table back-edge
/// does NOT exclude the asset (the asset's chosen downward parent is a deeper
/// ancestor, not the one under test), the marker is what keeps it out of the
/// ancestor's keep-children.
#[test]
fn asset_marker_excludes_a_child_the_back_edge_would_keep() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE artists (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE albums (id TEXT PRIMARY KEY, artist_id TEXT NOT NULL, \
             _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id)) STRICT",
    );
    c.execute_test_sql(
        "CREATE TABLE releases (id TEXT PRIMARY KEY, album_id TEXT NOT NULL, \
             managed INTEGER NOT NULL DEFAULT 0, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (album_id) REFERENCES albums (id)) STRICT",
    );
    // An asset that references BOTH ancestors; its chosen downward parent is
    // the deeper one (albums), so the back-edge only excludes it from albums,
    // never from artists.
    c.execute_test_sql(
        "CREATE TABLE artist_images (id TEXT PRIMARY KEY, artist_id TEXT NOT NULL, \
             album_id TEXT NOT NULL, _updated_at TEXT NOT NULL, \
             FOREIGN KEY (artist_id) REFERENCES artists (id), \
             FOREIGN KEY (album_id) REFERENCES albums (id)) STRICT",
    );

    let base = |asset: bool| {
        let image = SyncedTable::new(
            "artist_images",
            crate::protocol::synced_schema::RowIdentity::SharedKey,
        );
        let image = if asset { image.asset() } else { image };
        vec![
            SyncedTable::new(
                "releases",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .gated_by("managed"),
            SyncedTable::new(
                "albums",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .gated_by_descendants(),
            SyncedTable::new(
                "artists",
                crate::protocol::synced_schema::RowIdentity::SharedKey,
            )
            .gated_by_descendants(),
            image,
        ]
    };

    let without = Gates::from_tables(&c, &base(false)).expect("gates without asset marker");
    assert!(
            inferred_children(&without, "artists")
                .iter()
                .any(|(child, _)| child == "artist_images"),
            "without .asset(), the image is a keep-child of artists (the back-edge does not exclude it)"
        );

    let with = Gates::from_tables(&c, &base(true)).expect("gates with asset marker");
    assert!(
        !inferred_children(&with, "artists")
            .iter()
            .any(|(child, _)| child == "artist_images"),
        ".asset() excludes the image from artists' keep-children"
    );
}
