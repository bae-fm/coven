use super::inbound::*;
use super::routing::*;
use super::*;
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::session::{ConflictAction, Session};

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
             CREATE TABLE _coven_row_routes (
                 routing_id TEXT PRIMARY KEY,
                 table_name TEXT NOT NULL,
                 row_id TEXT NOT NULL,
                 _updated_at TEXT NOT NULL,
                 UNIQUE(table_name, row_id)
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

#[test]
fn inbound_circle_filter_keeps_only_rows_owned_by_its_winning_mirror() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
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
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'first', '1')",
            [&first_route],
        )
        .expect("insert first route");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'second', '1')",
            [&second_route],
        )
        .expect("insert second route");
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
        &note_gates(&target),
        &key,
    )
    .expect("filter first Circle package");
    let rows = crate::walk_changeset(&filtered).expect("walk filtered changeset");
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .any(|row| row.table == "notes" && row.pk() == Some("first")));
    assert!(rows
        .iter()
        .any(|row| { row.table == "_coven_row_routes" && row.pk() == Some(first_route.as_str()) }));
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
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("Circle package must not carry the Store mirror");
    assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
}

#[test]
fn inbound_circle_filter_rejects_a_route_for_an_unscoped_table() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("_coven_row_routes"))
        .expect("attach private routes");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES ('route', 'unknown', 'row', '1')",
            [],
        )
        .expect("insert undeclared route");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract route changeset");
    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let circle = CircleId::from_bytes([1; 16]);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
            [circle.to_string()],
        )
        .expect("install winning mirror");

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        circle,
        &StoreAudienceTransitions::default(),
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("Circle package route must name a scoped table");
    assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
}

#[test]
fn inbound_circle_filter_rejects_a_private_route_update() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    source
        .execute_batch(
            "CREATE TABLE tasks (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
        )
        .expect("create second scoped table");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES ('route', 'notes', 'row', '1')",
            [],
        )
        .expect("seed private route");
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("_coven_row_routes"))
        .expect("attach private routes");
    source
        .execute(
            "UPDATE _coven_row_routes
                 SET table_name = 'tasks', row_id = 'row2', _updated_at = '2'
                 WHERE routing_id = 'route'",
            [],
        )
        .expect("update private route");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract route update");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute_batch(
            "CREATE TABLE tasks (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
        )
        .expect("create second scoped table");
    let circle = CircleId::from_bytes([1; 16]);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
            [circle.to_string()],
        )
        .expect("install winning mirror");
    let gates = Gates::from_tables(
        &target,
        &[
            SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
            SyncedTable::new("tasks", RowIdentity::IndependentUuid).scoped_by("audience"),
        ],
    )
    .expect("build scoped gates");

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        circle,
        &StoreAudienceTransitions::default(),
        &gates,
        &routing_key(),
    )
    .expect_err("private routes must be complete INSERT images");
    assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
}

#[test]
fn inbound_circle_filter_rejects_a_private_route_delete() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES ('route', 'notes', 'row', '1')",
            [],
        )
        .expect("seed private route");
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("_coven_row_routes"))
        .expect("attach private routes");
    source
        .execute(
            "DELETE FROM _coven_row_routes WHERE routing_id = 'route'",
            [],
        )
        .expect("delete private route");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract route delete");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let circle = CircleId::from_bytes([1; 16]);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES ('route', ?1, '2')",
            [circle.to_string()],
        )
        .expect("install winning mirror");

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        circle,
        &StoreAudienceTransitions::default(),
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("private routes must be complete INSERT images");
    assert!(matches!(error, GateError::InvalidInboundAudiencePackage(_)));
}

#[test]
fn inbound_circle_filter_rejects_a_duplicate_private_route() {
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    // Two authenticated INSERT images for the same (table, row_id). A session
    // cannot capture both — the UNIQUE(table_name, row_id) constraint refuses
    // the second — so concatenate two single-route changesets to forge the
    // duplicate a malicious package could carry on the wire.
    let mut changeset = private_route_insert_changeset(&[(
        routing_id.clone(),
        "notes".to_string(),
        "row".to_string(),
        "1".to_string(),
    )])
    .expect("build first route image");
    changeset.extend(
        private_route_insert_changeset(&[(
            routing_id.clone(),
            "notes".to_string(),
            "row".to_string(),
            "1".to_string(),
        )])
        .expect("build second route image"),
    );

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let circle = CircleId::from_bytes([1; 16]);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '1')",
            (&routing_id, circle.to_string()),
        )
        .expect("install winning mirror");
    let transitions = store_transitions([(
        routing_id.clone(),
        Audience::Circle(circle),
        "1".to_string(),
    )]);

    let error = filter_inbound_circle_changeset(
        &target,
        &changeset,
        circle,
        &transitions,
        &note_gates(&target),
        &key,
    )
    .expect_err("a package must not carry two routes for one row");
    assert!(
        error
            .to_string()
            .contains("duplicate private route for notes.row"),
        "{error}"
    );
}

#[test]
fn inbound_private_route_must_authenticate_its_table_and_row() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
        .expect("insert scoped row");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            ["0".repeat(64)],
        )
        .expect("insert forged private route");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract forged route package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let error = normalize_inbound_store_changeset(
        &target,
        &changeset,
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("forged private route id must be rejected");
    assert!(error
        .to_string()
        .contains("does not authenticate notes.row"));
}

#[test]
fn inbound_private_route_must_accompany_its_complete_row_insert() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let mut session = Session::new(&source).expect("create source session");
    session
        .attach(Some("_coven_row_routes"))
        .expect("attach private routes");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            [&routing_id],
        )
        .expect("insert orphan private route");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract orphan route package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let error = normalize_inbound_store_changeset(&target, &changeset, &note_gates(&target), &key)
        .expect_err("orphan private route must be rejected");
    assert!(error.to_string().contains("has no complete row INSERT"));
}

#[test]
fn inbound_private_route_uses_its_audience_transition_stamp() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let circle = CircleId::from_bytes([1; 16]);
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [circle.to_string()],
        )
        .expect("insert scoped row with an older content stamp");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '2')",
            [&routing_id],
        )
        .expect("insert private route with the audience transition stamp");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract Circle package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    target
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, ?2, '2')",
            (&routing_id, circle.to_string()),
        )
        .expect("install winning audience transition");

    let filtered = filter_inbound_circle_changeset(
        &target,
        &changeset,
        circle,
        &store_transitions([(routing_id, Audience::Circle(circle), "2".to_string())]),
        &note_gates(&target),
        &key,
    )
    .expect("route stamp follows the audience transition, not row content");
    assert_eq!(
        crate::walk_changeset(&filtered)
            .expect("walk filtered Circle package")
            .len(),
        2
    );
}

#[test]
fn inbound_circle_filter_omits_an_authenticated_route_after_a_newer_move() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let old_circle = CircleId::from_bytes([1; 16]);
    let new_circle = CircleId::from_bytes([2; 16]);
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'old move', '1')",
            [old_circle.to_string()],
        )
        .expect("insert old destination row");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            [&routing_id],
        )
        .expect("insert old destination route");
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
fn inbound_private_route_must_match_its_store_transition_audience() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let package_circle = CircleId::from_bytes([1; 16]);
    let transition_circle = CircleId::from_bytes([2; 16]);
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [package_circle.to_string()],
        )
        .expect("insert packaged Circle row");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            [&routing_id],
        )
        .expect("insert packaged private route");
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
    for table in ["notes", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute(
            "INSERT INTO notes VALUES ('row', ?1, 'body', '1')",
            [row_circle.to_string()],
        )
        .expect("insert row for a different Circle");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
            [&routing_id],
        )
        .expect("insert private route");
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
        &note_gates(&target),
        &key,
    )
    .expect_err("a scoped row value must match its package audience");

    assert!(error
        .to_string()
        .contains("different audience than its row value"));
}

#[test]
fn inbound_private_route_is_rebuilt_as_canonical_text() {
    let source = Connection::open_in_memory().expect("open source");
    source
        .execute_batch(
            "CREATE TABLE notes (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     body TEXT,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE _coven_row_routes (
                     routing_id PRIMARY KEY,
                     table_name,
                     row_id,
                     _updated_at,
                     UNIQUE(table_name, row_id)
                 );
                 CREATE TABLE _coven_audience (
                     routing_id TEXT PRIMARY KEY,
                     circle_id TEXT,
                     _updated_at TEXT NOT NULL
                 );",
        )
        .expect("create source schema with untyped private routes");
    let key = routing_key();
    let routing_id = row_routing_id(&key, "notes", "row").to_string();
    let mut session = Session::new(&source).expect("create source session");
    for table in ["notes", "_coven_audience", "_coven_row_routes"] {
        session.attach(Some(table)).expect("attach source table");
    }
    source
        .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
        .expect("insert scoped row");
    source
        .execute(
            "INSERT INTO _coven_row_routes VALUES (?1, ?2, ?3, ?4)",
            (
                routing_id.as_bytes().to_vec(),
                b"notes".to_vec(),
                b"row".to_vec(),
                b"1".to_vec(),
            ),
        )
        .expect("insert byte-valued private route");
    source
        .execute(
            "INSERT INTO _coven_audience VALUES (?1, NULL, '1')",
            [&routing_id],
        )
        .expect("insert Store audience transition");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract byte-valued route package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let normalized =
        normalize_inbound_store_changeset(&target, &changeset, &note_gates(&target), &key)
            .expect("normalize authenticated private route");
    for part in [normalized.mirror, normalized.rows] {
        target
            .apply_strm(
                &mut &part[..],
                None::<fn(&str) -> bool>,
                |_conflict, _item| ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .expect("apply normalized package");
    }
    let types = target
        .query_row(
            "SELECT typeof(routing_id), typeof(table_name), typeof(row_id), typeof(_updated_at)
                 FROM _coven_row_routes",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .expect("read normalized private route types");
    assert_eq!(
        types,
        (
            "text".to_string(),
            "text".to_string(),
            "text".to_string(),
            "text".to_string(),
        )
    );
}

#[test]
fn inbound_scoped_row_insert_must_have_a_private_route() {
    let source = Connection::open_in_memory().expect("open source");
    routing_schema(&source);
    let mut session = Session::new(&source).expect("create source session");
    session.attach(Some("notes")).expect("attach scoped table");
    source
        .execute("INSERT INTO notes VALUES ('row', NULL, 'body', '1')", [])
        .expect("insert unbound scoped row");
    let mut changeset = Vec::new();
    session
        .changeset_strm(&mut changeset)
        .expect("extract unbound row package");

    let target = Connection::open_in_memory().expect("open target");
    routing_schema(&target);
    let error = normalize_inbound_store_changeset(
        &target,
        &changeset,
        &note_gates(&target),
        &routing_key(),
    )
    .expect_err("unbound scoped row must be rejected");
    assert!(error.to_string().contains("has no private route"));
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
        "INSERT INTO _coven_row_routes VALUES (?1, 'notes', 'row', '1')",
        [&routing_id],
    )
    .expect("insert private route at the audience-transition stamp");
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
    conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE comments (
                 id TEXT PRIMARY KEY,
                 note_id TEXT NOT NULL REFERENCES notes(id),
                 body TEXT,
                 _updated_at TEXT NOT NULL
             ) STRICT;
             INSERT INTO notes VALUES ('local', 'local', 'local', '1');
             INSERT INTO comments VALUES ('local-child', 'local', 'local', '1');
             INSERT INTO _coven_row_routes VALUES ('local-route', 'notes', 'local', '1');
             INSERT INTO _coven_row_routes VALUES ('local-child-route', 'comments', 'local-child', '1');
             INSERT INTO _coven_row_routes VALUES ('orphan-route', 'notes', 'absent', '1');",
        )
        .expect("install scoped rows");
    conn.execute(
        "INSERT INTO notes VALUES ('stale', ?1, 'stale', '1')",
        [CircleId::from_bytes([1; 16]).to_string()],
    )
    .expect("install stale root");
    conn.execute_batch(
            "INSERT INTO comments VALUES ('stale-child', 'stale', 'stale', '1');
             INSERT INTO _coven_row_routes VALUES ('stale-route', 'notes', 'stale', '1');
             INSERT INTO _coven_row_routes VALUES ('stale-child-route', 'comments', 'stale-child', '1');",
        )
        .expect("install stale subtree");
    let inactive = CircleId::from_bytes([2; 16]);
    conn.execute(
        "INSERT INTO notes VALUES ('inactive', ?1, 'inactive', '1')",
        [inactive.to_string()],
    )
    .expect("install inactive root");
    conn.execute(
        "INSERT INTO _coven_row_routes VALUES ('inactive-route', 'notes', 'inactive', '1')",
        [],
    )
    .expect("install inactive route");
    conn.execute(
        "INSERT INTO _coven_audience VALUES ('inactive-route', ?1, '1')",
        [inactive.to_string()],
    )
    .expect("install matching inactive mirror");
    let tables = vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
        SyncedTable::new("comments", RowIdentity::IndependentUuid)
            .inherits_audience_through("note_id"),
    ];
    let gates = Gates::from_tables(&conn, &tables).expect("build scoped gates");

    prune_ineligible_scoped_rows(&conn, &gates, &BTreeSet::from([inactive]))
        .expect("prune stale scoped rows");

    let notes: i64 = conn
        .query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))
        .expect("count notes");
    let comments: i64 = conn
        .query_row("SELECT COUNT(*) FROM comments", [], |row| row.get(0))
        .expect("count comments");
    let routes: i64 = conn
        .query_row("SELECT COUNT(*) FROM _coven_row_routes", [], |row| {
            row.get(0)
        })
        .expect("count routes");
    assert_eq!((notes, comments, routes), (1, 1, 2));
}
