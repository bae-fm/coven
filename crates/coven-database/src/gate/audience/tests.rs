use super::inbound::*;
use super::routing::*;
use super::*;
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::session::Session;

fn routing_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE notes (
                 id TEXT PRIMARY KEY,
                 audience TEXT,
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE _coven_audience (
                 routing_id TEXT PRIMARY KEY,
                 circle_id TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE row_blob_locators (
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 column_name TEXT NOT NULL,
                 row_stamp TEXT NOT NULL,
                 audience_authority TEXT NOT NULL CHECK (json_valid(audience_authority)),
                 remote_object_id TEXT NOT NULL CHECK (length(remote_object_id) = 64),
                 PRIMARY KEY (table_name, row_id, column_name, row_stamp)
             ) STRICT;",
    )
    .expect("create inbound audience test schema");
}

fn note_gates(conn: &Connection) -> Gates {
    Gates::from_tables(
        conn,
        &[SyncedTable::new("notes", RowIdentity::SharedKey).scoped_by("audience")],
    )
    .expect("build scoped gates")
}

fn routing_key() -> RowRoutingKey {
    coven_protocol::circle::derive_row_routing_key(
        &coven_keys::encryption::EncryptionService::from_key([7; 32]),
        coven_protocol::store_commit::ObjectHash::digest(b"audience test"),
    )
    .expect("derive test row-routing key")
}

fn store_transitions(
    transitions: impl IntoIterator<Item = (String, Audience, String)>,
) -> StoreAudienceTransitions {
    StoreAudienceTransitions {
        by_routing_id: transitions
            .into_iter()
            .map(|(routing_id, audience, stamp)| (routing_id, (audience, stamp)))
            .collect(),
    }
}

fn no_held_rows() -> BTreeSet<(String, String)> {
    BTreeSet::new()
}

/// Capturing a write's routing is the write that installs its audience mirror,
/// and it publishes that mirror by snapshotting what it just wrote — so it runs
/// exactly once per write. A second pass over the same write re-upserts the rows
/// it already wrote, sees no change, and hands back an empty mirror: the moved
/// rows would reach their destination audience with nothing telling the devices
/// there that they moved. Whatever a write has to decide between capture and
/// partition (an audience move's blob row stamps, today) is read separately and
/// this runs after it.
#[test]
fn capturing_a_write_s_routing_publishes_its_mirror_once() {
    let conn = Connection::open_in_memory().expect("open connection");
    routing_schema(&conn);
    let gates = note_gates(&conn);
    let key = routing_key();
    let circle = CircleId::from_bytes([3; 16]);
    conn.execute("INSERT INTO notes VALUES ('moved', NULL, 'body', '1')", [])
        .expect("insert the note in the Store audience");
    let mut session = Session::new(&conn).expect("create session");
    session.attach(Some("notes")).expect("attach notes");
    conn.execute(
        "UPDATE notes SET audience = ?1, _updated_at = '2' WHERE id = 'moved'",
        [circle.to_string()],
    )
    .expect("move the note into the Circle");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract the move changeset");
    drop(session);

    let first = capture_routing_changes(&conn, &changeset, &gates, &key)
        .expect("capture the move's routing");
    let mirror = crate::walk_changeset(&first.store_mirror).expect("walk the Store mirror");
    assert_eq!(
        mirror.len(),
        1,
        "the move publishes the moved row's audience mirror: {mirror:?}",
    );

    let again = capture_routing_changes(&conn, &changeset, &gates, &key)
        .expect("capture the same move's routing again");
    assert!(
        crate::walk_changeset(&again.store_mirror)
            .expect("walk the repeated Store mirror")
            .is_empty(),
        "a second capture has nothing left to publish",
    );
}

/// A deleted row's audience is the one its public mirror names. The row itself
/// is gone by the time routing is captured, so nothing else on the device still
/// says where the deletion has to travel.
#[test]
fn a_deleted_scoped_row_reports_its_mirrored_audience() {
    let conn = Connection::open_in_memory().expect("open connection");
    routing_schema(&conn);
    let gates = note_gates(&conn);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    conn.execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
        .expect("insert the Store row");
    conn.execute(
        "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
        [&routing_id],
    )
    .expect("install the Store audience mirror");
    let mut session = Session::new(&conn).expect("create session");
    session.attach(Some("notes")).expect("attach notes");
    conn.execute("DELETE FROM notes WHERE id = 'row'", [])
        .expect("delete the Store row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract the delete");
    drop(session);

    let routing =
        capture_routing_changes(&conn, &changeset, &gates, &key).expect("capture the deletion");
    assert_eq!(
        routing
            .deleted_rows
            .get(&("notes".to_string(), "row".to_string())),
        Some(&Audience::Store),
    );
    let mirrors: i64 = conn
        .query_row("SELECT COUNT(*) FROM _coven_audience", [], |row| row.get(0))
        .expect("count mirrors");
    assert_eq!(mirrors, 0, "the deletion removes the row's mirror");
}

/// One write can move a scoped root to Local and delete one of its children.
/// The child's deletion still belongs to the audience the child was in, which
/// the mirror records — the root's post-write value describes where the rest of
/// the component went, not where this row's removal has to be published.
#[test]
fn a_child_deleted_while_its_parent_moves_local_keeps_its_prior_audience() {
    let conn = Connection::open_in_memory().expect("open connection");
    routing_schema(&conn);
    conn.execute_batch(
        "CREATE TABLE comments (
             id TEXT PRIMARY KEY,
             note_id TEXT NOT NULL REFERENCES notes(id),
             body TEXT,
             _updated_at TEXT NOT NULL
         ) STRICT;",
    )
    .expect("create the child table");
    let gates = Gates::from_tables(
        &conn,
        &[
            SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
            SyncedTable::new("comments", RowIdentity::IndependentUuid)
                .inherits_audience_through("note_id"),
        ],
    )
    .expect("build scoped gates");
    let key = routing_key();
    conn.execute_batch(
        "INSERT INTO notes VALUES ('root', NULL, 'root', '1');
         INSERT INTO comments VALUES ('child', 'root', 'child', '1');",
    )
    .expect("install the Store subtree");
    for (table, row_id) in [("notes", "root"), ("comments", "child")] {
        conn.execute(
            "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
            [row_routing_id(&key, table, row_id).to_string()],
        )
        .expect("install the Store audience mirror");
    }
    let mut session = Session::new(&conn).expect("create session");
    for table in ["notes", "comments"] {
        session.attach(Some(table)).expect("attach table");
    }
    conn.execute_batch(
        "DELETE FROM comments WHERE id = 'child';
         UPDATE notes SET audience = 'local', _updated_at = '2' WHERE id = 'root';",
    )
    .expect("retire the child and take the root private");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract the write");
    drop(session);

    let routing =
        capture_routing_changes(&conn, &changeset, &gates, &key).expect("capture the write");
    assert_eq!(
        routing
            .deleted_rows
            .get(&("comments".to_string(), "child".to_string())),
        Some(&Audience::Store),
    );
}

/// A deleted row with no mirror never had a public audience, which is only true
/// of a Local row. A missing mirror for a row that was in one is a state no
/// later pass can reconstruct, so the capture refuses it.
#[test]
fn a_deleted_scoped_row_without_a_mirror_must_have_been_local() {
    let key = routing_key();
    let delete_note = |audience: Option<&str>| {
        let conn = Connection::open_in_memory().expect("open connection");
        routing_schema(&conn);
        let gates = note_gates(&conn);
        conn.execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [audience],
        )
        .expect("insert the row");
        let mut session = Session::new(&conn).expect("create session");
        session.attach(Some("notes")).expect("attach notes");
        conn.execute("DELETE FROM notes WHERE id = 'row'", [])
            .expect("delete the row");
        let mut changeset = Vec::new();
        session
            .changeset_strm(&mut changeset)
            .expect("extract the delete");
        drop(session);
        capture_routing_changes(&conn, &changeset, &gates, &key).map(|routing| routing.deleted_rows)
    };

    let error = delete_note(None).expect_err("an unmirrored Store row must be refused");
    assert!(
        matches!(
            &error,
            GateError::UnmirroredDeletedRow {
                table,
                row_id,
                audience,
            } if table == "notes" && row_id == "row" && audience == &Audience::Store
        ),
        "{error}",
    );

    let deleted = delete_note(Some("local")).expect("a Local row needs no mirror");
    assert_eq!(
        deleted.get(&("notes".to_string(), "row".to_string())),
        Some(&Audience::Local),
    );
}

#[test]
fn inbound_circle_filter_keeps_only_rows_owned_by_its_winning_mirror() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source table");
    let first = CircleId::from_bytes([1; 16]);
    let second = CircleId::from_bytes([2; 16]);
    let key = routing_key();
    let first_route = row_routing_id(&key, "notes", "first").to_string();
    let second_route = row_routing_id(&key, "notes", "second").to_string();
    source
        .execute(
            "INSERT INTO notes VALUES (?1, ?2, 'first', '1')",
            ("first", first.to_string()),
        )
        .expect("insert first note");
    source
        .execute(
            "INSERT INTO notes VALUES (?1, ?2, 'second', '1')",
            ("second", first.to_string()),
        )
        .expect("insert second note");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract source changeset");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
            (&first_route, first.to_string()),
        )
        .expect("install first mirror");
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
            (&second_route, second.to_string()),
        )
        .expect("install second mirror");

    let transitions = store_transitions([
        (
            first_route.clone(),
            Audience::Circle(first),
            "1".to_string(),
        ),
        (
            second_route.clone(),
            Audience::Circle(first),
            "1".to_string(),
        ),
    ]);
    let filtered = filter_inbound_circle_changeset(
        &target,
        &changeset,
        first,
        &transitions,
        &no_held_rows(),
        &note_gates(&target),
        &key,
    )
    .expect("filter first Circle package");
    let rows = crate::walk_changeset(&filtered).expect("walk filtered changeset");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(rows
        .iter()
        .any(|row| row.table == "notes" && row.pk() == Some("first")));
}

#[test]
fn inbound_circle_filter_rejects_a_store_mirror_change() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("_coven_audience"))
        .expect("attach audience mirror");
    source
        .execute(
            "INSERT INTO _coven_audience VALUES ('route', NULL, '1')",
            [],
        )
        .expect("insert mirror");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract mirror changeset");
    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        CircleId::from_bytes([1; 16]),
        &StoreAudienceTransitions::default(),
        &no_held_rows(),
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("Circle package must not carry the Store mirror");
    assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
}

/// A package states rows of the tables the receiver declares, and the audience
/// mirror. Anything else names a table this device has no routing contract for.
#[test]
fn inbound_package_rejects_an_undeclared_table() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    source
        .execute_batch(
            "CREATE TABLE strays (
                 id TEXT PRIMARY KEY,
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;",
        )
        .expect("create an undeclared table");
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("strays"))
        .expect("attach the undeclared table");
    source
        .execute("INSERT INTO strays VALUES ('row', 'body', '1')", [])
        .expect("insert an undeclared row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract the undeclared changeset");
    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        CircleId::from_bytes([1; 16]),
        &StoreAudienceTransitions::default(),
        &no_held_rows(),
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("a package must only name declared tables");
    assert!(
        error
            .to_string()
            .contains("package names undeclared table strays"),
        "{error}",
    );
}

#[test]
fn inbound_scoped_insert_is_omitted_after_a_newer_move() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let old_circle = CircleId::from_bytes([1; 16]);
    let new_circle = CircleId::from_bytes([2; 16]);
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source table");
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'old move', '1')",
            [old_circle.to_string()],
        )
        .expect("insert old destination row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract old Circle package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
            (&routing_id, new_circle.to_string()),
        )
        .expect("install newer winning move");
    let filtered = filter_inbound_circle_changeset(
        &target,
        &changeset,
        old_circle,
        &store_transitions([(routing_id, Audience::Circle(old_circle), "1".to_string())]),
        &no_held_rows(),
        &note_gates(&target),
        &key,
    )
    .expect("authenticate the old package before omitting it");

    assert!(crate::walk_changeset(&filtered)
        .expect("walk omitted package")
        .is_empty());
}

#[test]
fn inbound_store_filter_omits_a_stale_edit_after_a_circle_move() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    source
        .execute("INSERT INTO notes VALUES ('row', NULL, 'base', '1')", [])
        .expect("insert source Store row");
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source row");
    source
        .execute(
            "UPDATE notes SET body = 'stale edit', _updated_at = '2' WHERE id = 'row'",
            [],
        )
        .expect("edit source Store row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract Store edit");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '3')",
            (&routing_id, CircleId::from_bytes([1; 16]).to_string()),
        )
        .expect("install winning Circle move");
    let filtered = filter_inbound_store_rows(&target, &changeset, &note_gates(&target), &key)
        .expect("filter stale Store edit");

    assert!(crate::walk_changeset(&filtered)
        .expect("walk omitted Store edit")
        .is_empty());
}

#[test]
fn inbound_scoped_insert_must_match_its_store_transition_audience() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let package_circle = CircleId::from_bytes([1; 16]);
    let transition_circle = CircleId::from_bytes([2; 16]);
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source table");
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [package_circle.to_string()],
        )
        .expect("insert packaged Circle row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract Circle package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
            (&routing_id, package_circle.to_string()),
        )
        .expect("install package Circle as the current winner");
    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        package_circle,
        &store_transitions([(
            routing_id,
            Audience::Circle(transition_circle),
            "1".to_string(),
        )]),
        &no_held_rows(),
        &note_gates(&target),
        &key,
    )
    .expect_err("a package must match its own Store transition audience");

    assert!(error
        .to_string()
        .contains("packaged for a different audience"));
}

#[test]
fn inbound_scoped_row_must_match_its_package_audience() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let package_circle = CircleId::from_bytes([1; 16]);
    let row_circle = CircleId::from_bytes([2; 16]);
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source table");
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [row_circle.to_string()],
        )
        .expect("insert row for a different Circle");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract malformed Circle package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
            (&routing_id, package_circle.to_string()),
        )
        .expect("install package Circle as the current winner");
    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        package_circle,
        &store_transitions([(
            routing_id,
            Audience::Circle(package_circle),
            "1".to_string(),
        )]),
        &no_held_rows(),
        &note_gates(&target),
        &key,
    )
    .expect_err("a scoped row value must match its package audience");

    assert!(error
        .to_string()
        .contains("different audience than its row value"));
}

/// A scoped row INSERT never establishes the row's identity by itself. Without
/// an audience transition in the same Store package, the receiver has to
/// already know the row: a mirror, the live row, or a row a replay holds and
/// has yet to re-materialize. Otherwise a package could name any row id and
/// have it land.
#[test]
fn inbound_scoped_insert_without_a_transition_needs_a_prior_identity() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let circle = CircleId::from_bytes([1; 16]);
    let other_circle = CircleId::from_bytes([2; 16]);
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach source table");
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [circle.to_string()],
        )
        .expect("insert the packaged row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract the package");

    let filter = |target: &Connection, held: &BTreeSet<(String, String)>| {
        filter_inbound_circle_changeset(
            target,
            &changeset,
            circle,
            &StoreAudienceTransitions::default(),
            held,
            &note_gates(target),
            &key,
        )
    };

    let unknown = Connection::open_in_memory().expect("open target");
    routing_schema(&unknown);
    let error = filter(&unknown, &no_held_rows())
        .expect_err("a row this device has never held must be refused");
    assert!(
        error.to_string().contains(
            "scoped row INSERT notes.row has no Store audience transition and no prior identity"
        ),
        "{error}",
    );

    let mirrored = Connection::open_in_memory().expect("open target");
    routing_schema(&mirrored);
    mirrored
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
            (&routing_id, other_circle.to_string()),
        )
        .expect("install a mirror for another audience");
    assert!(
        crate::walk_changeset(
            &filter(&mirrored, &no_held_rows()).expect("a mirrored row has an identity")
        )
        .expect("walk the filtered package")
        .is_empty(),
        "the winning mirror names another audience, so the row is omitted",
    );

    let local = Connection::open_in_memory().expect("open target");
    routing_schema(&local);
    local
        .execute("INSERT INTO notes VALUES ('row', 'local', 'mine', '1')", [])
        .expect("hold the row Local");
    assert!(
        crate::walk_changeset(&filter(&local, &no_held_rows()).expect("a live row is an identity"))
            .expect("walk the filtered package")
            .is_empty(),
        "a Local row has no winning mirror, so the package is omitted",
    );

    let holding = Connection::open_in_memory().expect("open target");
    routing_schema(&holding);
    let held = BTreeSet::from([("notes".to_string(), "row".to_string())]);
    filter(&holding, &held).expect("a held private row carries the identity through its replay");
}

#[test]
fn store_snapshot_routing_stamp_is_independent_from_content_stamp() {
    let conn = Connection::open_in_memory().expect("open snapshot");
    routing_schema(&conn);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    conn.execute("INSERT INTO notes VALUES ('row', NULL, 'edited', '2')", [])
        .expect("insert content-edited Store row");
    conn.execute(
        "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
        [&routing_id],
    )
    .expect("insert Store mirror at the audience-transition stamp");

    validate_snapshot_routing_state(&conn, &note_gates(&conn), &key, &Audience::Store)
        .expect("content-only edits must not invalidate unchanged routing");
}

#[test]
fn audience_prune_removes_stale_scoped_subtrees_and_keeps_local_rows() {
    let conn = Connection::open_in_memory().expect("open target");
    routing_schema(&conn);
    let key = routing_key();
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id),
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO notes VALUES ('local', 'local', 'local', '1');
             INSERT INTO comments VALUES ('local-child', 'local', 'local', '1');",
    )
    .expect("install scoped rows");
    conn.execute(
        "INSERT INTO notes VALUES ('stale', ?1, 'stale', '1')",
        [CircleId::from_bytes([1; 16]).to_string()],
    )
    .expect("install stale root");
    conn.execute_batch("INSERT INTO comments VALUES ('stale-child', 'stale', 'stale', '1');")
        .expect("install stale subtree");
    let inactive = CircleId::from_bytes([2; 16]);
    conn.execute(
        "INSERT INTO notes VALUES ('inactive', ?1, 'inactive', '1')",
        [inactive.to_string()],
    )
    .expect("install inactive root");
    conn.execute(
        "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
        (
            row_routing_id(&key, "notes", "inactive").to_string(),
            inactive.to_string(),
        ),
    )
    .expect("install matching inactive mirror");
    let tables = vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
        SyncedTable::new("comments", RowIdentity::IndependentUuid)
            .inherits_audience_through("note_id"),
    ];
    let gates = Gates::from_tables(&conn, &tables).expect("build scoped gates");

    prune_ineligible_scoped_rows(&conn, &gates, &BTreeSet::from([inactive]), Some(&key))
        .expect("prune stale scoped rows");

    let notes: i64 = conn
        .query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
        .expect("count notes");
    let comments: i64 = conn
        .query_row("SELECT COUNT(*) FROM comments", [], |row| row.get(0))
        .expect("count comments");
    assert_eq!((notes, comments), (1, 1));
}
