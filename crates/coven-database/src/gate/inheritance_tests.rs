//! How a plain table's gate parent is resolved: it is declared, never elected.
//! These cover the classification itself — which declarations are accepted, which
//! are refused, and what a declared chain publishes — while `tests.rs` covers what
//! the resulting gate does to a changeset.

use super::tests::{conn, downward_parent, ConnectionTestSql};
use super::{GateError, Gates};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::Connection;

fn plain(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::SharedKey)
}

fn build_error(c: &Connection, tables: &[SyncedTable]) -> GateError {
    match Gates::from_tables(c, tables) {
        Ok(_) => panic!("the gate model must refuse this declaration set"),
        Err(error) => error,
    }
}

// ---- a gated reference must name the foreign key it inherits through -------

#[test]
fn a_scoped_descendant_requires_a_declared_gate_parent() {
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

    let error = build_error(
        &c,
        &[plain("notes").scoped_by("audience"), plain("note_tags")],
    );
    assert!(
        matches!(&error, GateError::MissingGateParentDeclaration { table } if table == "note_tags"),
        "{error}"
    );
    assert!(
        error.to_string().contains(
            "table note_tags references a gated table and must declare the foreign key it \
             inherits its gate through"
        ),
        "{error}"
    );

    let gates = Gates::from_tables(
        &c,
        &[
            plain("notes").scoped_by("audience"),
            plain("note_tags").inherits_audience_through("note_id"),
        ],
    )
    .expect("build explicitly declared audience inheritance");
    assert!(gates.tables.contains_key("notes"));
    assert_eq!(
        downward_parent(&gates, "note_tags"),
        ("notes".to_string(), "note_id".to_string()),
    );
}

#[test]
fn an_undeclared_join_row_referencing_a_gated_root_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE roots (
                id TEXT PRIMARY KEY,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
             CREATE TABLE children (
                id TEXT PRIMARY KEY,
                a_root_id TEXT NOT NULL REFERENCES roots (id),
                z_root_id TEXT NOT NULL REFERENCES roots (id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );

    let error = build_error(&c, &[plain("roots").gated_by("shared"), plain("children")]);
    assert!(
        matches!(&error, GateError::MissingGateParentDeclaration { table } if table == "children"),
        "two foreign keys to the same root are still two answers, and coven states \
         neither for the host: {error}"
    );

    for column in ["a_root_id", "z_root_id"] {
        let gates = Gates::from_tables(
            &c,
            &[
                plain("roots").gated_by("shared"),
                plain("children").inherits_audience_through(column),
            ],
        )
        .expect("either declaration builds");
        assert_eq!(
            downward_parent(&gates, "children"),
            ("roots".to_string(), column.to_string()),
        );
    }
}

#[test]
fn an_undeclared_table_referencing_only_ungated_tables_stays_unconditional() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE tags (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE tag_synonyms (
                id TEXT PRIMARY KEY,
                tag_id TEXT NOT NULL REFERENCES tags (id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );

    let gates =
        Gates::from_tables(&c, &[plain("tags"), plain("tag_synonyms")]).expect("build gates");

    assert!(
        gates.tables.is_empty(),
        "neither table carries a gate, so neither is in the gate map and both sync \
         unconditionally",
    );
}

// ---- the declared edge is followed, whatever the graph looks like ---------

#[test]
fn a_declared_ancestor_edge_is_followed_whatever_its_depth() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE aouter (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE zinner (
                id TEXT PRIMARY KEY,
                aouter_id TEXT,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (aouter_id) REFERENCES aouter (id)
             ) STRICT;
         CREATE TABLE zgated (
                id TEXT PRIMARY KEY,
                zinner_id TEXT NOT NULL,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (zinner_id) REFERENCES zinner (id)
             ) STRICT;
         CREATE TABLE joiner (
                id TEXT PRIMARY KEY,
                aouter_id TEXT NOT NULL,
                zinner_id TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (aouter_id) REFERENCES aouter (id),
                FOREIGN KEY (zinner_id) REFERENCES zinner (id)
             ) STRICT;",
    );
    let with_joiner_through = |column: &str| {
        vec![
            plain("aouter").gated_by_descendants(),
            plain("zinner").gated_by_descendants(),
            plain("zgated").gated_by("shared"),
            plain("joiner").inherits_audience_through(column),
        ]
    };

    let shallow =
        Gates::from_tables(&c, &with_joiner_through("aouter_id")).expect("declare the outer edge");
    assert_eq!(
        downward_parent(&shallow, "joiner"),
        ("aouter".to_string(), "aouter_id".to_string()),
        "the shallower ancestor is the parent when the host names its foreign key",
    );

    let deep =
        Gates::from_tables(&c, &with_joiner_through("zinner_id")).expect("declare the inner edge");
    assert_eq!(
        downward_parent(&deep, "joiner"),
        ("zinner".to_string(), "zinner_id".to_string()),
        "and the deeper one is, on the same schema, when the host names that one",
    );
}

#[test]
fn a_chain_of_plain_tables_reaches_the_root_through_each_declaration() {
    let c = conn();
    create_audio_format_schema(&c);

    let gates = Gates::from_tables(&c, &audio_format_tables()).expect("build gates");

    assert_eq!(
        downward_parent(&gates, "audio_formats"),
        ("tracks".to_string(), "track_id".to_string()),
    );
    assert_eq!(
        downward_parent(&gates, "audio_format_segments"),
        ("audio_formats".to_string(), "audio_format_id".to_string()),
        "a plain table inherits from another plain table, which inherits from the \
         track, which inherits from the release root",
    );
}

// ---- declarations the gate model refuses ----------------------------------

#[test]
fn a_declaration_naming_a_column_with_no_foreign_key_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE roots (
                id TEXT PRIMARY KEY,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE children (
                id TEXT PRIMARY KEY,
                root_id TEXT NOT NULL REFERENCES roots (id),
                note TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );

    let error = build_error(
        &c,
        &[
            plain("roots").gated_by("shared"),
            plain("children").inherits_audience_through("note"),
        ],
    );
    assert!(
        error
            .to_string()
            .contains("no foreign key uses that child column"),
        "{error}"
    );
}

#[test]
fn a_declaration_naming_a_column_two_foreign_keys_use_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE roots (
                id TEXT PRIMARY KEY,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE mirrors (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE children (
                id TEXT PRIMARY KEY,
                root_id TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (root_id) REFERENCES roots (id),
                FOREIGN KEY (root_id) REFERENCES mirrors (id)
             ) STRICT;",
    );

    let error = build_error(
        &c,
        &[
            plain("roots").gated_by("shared"),
            plain("mirrors"),
            plain("children").inherits_audience_through("root_id"),
        ],
    );
    assert!(
        error
            .to_string()
            .contains("2 foreign keys use that child column"),
        "a column two relationships share names no single parent: {error}"
    );
}

#[test]
fn a_declaration_pointing_at_an_undeclared_table_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE local_notes (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE children (
                id TEXT PRIMARY KEY,
                note_id TEXT NOT NULL REFERENCES local_notes (id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );

    let error = build_error(
        &c,
        &[plain("children").inherits_audience_through("note_id")],
    );
    assert!(
        error
            .to_string()
            .contains("its foreign key targets undeclared table local_notes"),
        "{error}"
    );
}

#[test]
fn a_declaration_selecting_a_composite_relationship_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE roots (
                left_key TEXT NOT NULL,
                right_key TEXT NOT NULL,
                id TEXT PRIMARY KEY,
                shared INTEGER NOT NULL,
                _updated_at TEXT NOT NULL,
                UNIQUE (left_key, right_key)
             ) STRICT;
         CREATE TABLE children (
                id TEXT PRIMARY KEY,
                root_left TEXT NOT NULL,
                root_right TEXT NOT NULL,
                _updated_at TEXT NOT NULL,
                FOREIGN KEY (root_left, root_right) REFERENCES roots (left_key, right_key)
             ) STRICT;",
    );

    let error = build_error(
        &c,
        &[
            plain("roots").gated_by("shared"),
            plain("children").inherits_audience_through("root_left"),
        ],
    );
    assert!(
        matches!(
            &error,
            GateError::CompositeGateForeignKey { table, parent }
                if table == "children" && parent == "roots"
        ),
        "{error}"
    );
}

#[test]
fn a_declaration_on_a_root_or_an_ancestor_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE albums (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE releases (
                id TEXT PRIMARY KEY,
                album_id TEXT NOT NULL REFERENCES albums (id),
                managed INTEGER NOT NULL DEFAULT 0,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );
    let roles = [
        plain("releases")
            .gated_by("managed")
            .inherits_audience_through("album_id"),
        plain("releases")
            .scoped_by("audience")
            .inherits_audience_through("album_id"),
        plain("releases")
            .remote_root()
            .inherits_audience_through("album_id"),
        plain("releases")
            .gated_by_descendants()
            .inherits_audience_through("album_id"),
    ];

    for releases in roles {
        let error = build_error(&c, &[plain("albums").gated_by_descendants(), releases]);
        assert!(
            error
                .to_string()
                .contains("only a plain descendant table may select an audience parent"),
            "{error}"
        );
    }
}

#[test]
fn a_declared_chain_that_reaches_no_terminus_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE tags (id TEXT PRIMARY KEY, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE tag_synonyms (
                id TEXT PRIMARY KEY,
                tag_id TEXT NOT NULL REFERENCES tags (id),
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );

    let error = build_error(
        &c,
        &[
            plain("tags"),
            plain("tag_synonyms").inherits_audience_through("tag_id"),
        ],
    );
    assert!(
        error.to_string().contains(
            "the selected foreign-key chain does not end at a gate root or kept ancestor"
        ),
        "a declaration that resolves to an ungated table inherits nothing: {error}"
    );
}

#[test]
fn a_declared_cycle_is_refused() {
    let c = conn();
    c.execute_test_sql(
        "CREATE TABLE first (
                id TEXT PRIMARY KEY,
                second_id TEXT REFERENCES second (id),
                _updated_at TEXT NOT NULL
             );
         CREATE TABLE second (
                id TEXT PRIMARY KEY,
                first_id TEXT REFERENCES first (id),
                _updated_at TEXT NOT NULL
             );",
    );

    let error = build_error(
        &c,
        &[
            plain("first").inherits_audience_through("second_id"),
            plain("second").inherits_audience_through("first_id"),
        ],
    );
    assert!(
        error.to_string().contains(
            "the selected foreign-key chain does not end at a gate root or kept ancestor"
        ),
        "each table inherits from the other, so neither reaches a gate: {error}"
    );
}

// ---- the shapes bae declares ---------------------------------------------

/// Works and artists are ancestors kept by what a managed release records; a
/// work's credits reach it through the artist, its parts through the child work.
fn work_graph_tables() -> Vec<SyncedTable> {
    vec![
        plain("releases").gated_by("managed"),
        plain("tracks").inherits_audience_through("release_id"),
        plain("works").gated_by_descendants(),
        plain("artists").gated_by_descendants(),
        plain("track_works").inherits_audience_through("track_id"),
        plain("track_artists").inherits_audience_through("track_id"),
        plain("work_artists").inherits_audience_through("artist_id"),
        plain("work_parts").inherits_audience_through("child_work_id"),
    ]
}

fn create_work_graph_schema(c: &Connection) {
    c.execute_test_sql(
        "CREATE TABLE releases (
                id TEXT PRIMARY KEY,
                managed INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE tracks (
                id TEXT PRIMARY KEY,
                release_id TEXT NOT NULL REFERENCES releases (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE works (id TEXT PRIMARY KEY, title TEXT, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE artists (id TEXT PRIMARY KEY, name TEXT, _updated_at TEXT NOT NULL) STRICT;
         CREATE TABLE track_works (
                id TEXT PRIMARY KEY,
                track_id TEXT NOT NULL REFERENCES tracks (id) ON DELETE CASCADE,
                work_id TEXT NOT NULL REFERENCES works (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE track_artists (
                id TEXT PRIMARY KEY,
                track_id TEXT NOT NULL REFERENCES tracks (id) ON DELETE CASCADE,
                artist_id TEXT NOT NULL REFERENCES artists (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE work_artists (
                id TEXT PRIMARY KEY,
                work_id TEXT NOT NULL REFERENCES works (id) ON DELETE CASCADE,
                artist_id TEXT NOT NULL REFERENCES artists (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE work_parts (
                id TEXT PRIMARY KEY,
                parent_work_id TEXT NOT NULL REFERENCES works (id) ON DELETE CASCADE,
                child_work_id TEXT NOT NULL REFERENCES works (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );
}

/// One release with a track that records `W_PART`, credited to `AR`. `W_WHOLE`
/// is the collection `W_PART` belongs to: nothing records or credits it, so only
/// the `work_parts` row names it.
fn seed_work_graph(c: &Connection, managed: u8) {
    c.execute_test_sql(&format!(
        "INSERT INTO releases (id, managed, _updated_at) VALUES ('R', {managed}, '0000000001000-0000-dev1');
         INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1');
         INSERT INTO works (id, title, _updated_at) VALUES ('W_PART', 'Sonata', '0000000001000-0000-dev1');
         INSERT INTO works (id, title, _updated_at) VALUES ('W_WHOLE', 'The Real Book', '0000000001000-0000-dev1');
         INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Composer', '0000000001000-0000-dev1');
         INSERT INTO track_works (id, track_id, work_id, _updated_at) VALUES ('TW', 'T', 'W_PART', '0000000001000-0000-dev1');
         INSERT INTO track_artists (id, track_id, artist_id, _updated_at) VALUES ('TA', 'T', 'AR', '0000000001000-0000-dev1');
         INSERT INTO work_artists (id, work_id, artist_id, _updated_at) VALUES ('WA', 'W_PART', 'AR', '0000000001000-0000-dev1');
         INSERT INTO work_parts (id, parent_work_id, child_work_id, _updated_at) VALUES ('WP', 'W_WHOLE', 'W_PART', '0000000001000-0000-dev1');"
    ));
}

#[test]
fn the_work_graphs_keep_children_follow_the_declarations() {
    let c = conn();
    create_work_graph_schema(&c);
    let gates = Gates::from_tables(&c, &work_graph_tables()).expect("gates");

    assert_eq!(
        super::tests::inferred_children(&gates, "works"),
        vec![
            ("track_works".to_string(), "work_id".to_string()),
            ("work_artists".to_string(), "work_id".to_string()),
        ],
        "a work is kept by the tracks that record it and the credits on it; \
         `work_parts` declares a work as its parent, so it never keeps one alive",
    );
    assert_eq!(
        super::tests::inferred_children(&gates, "artists"),
        vec![("track_artists".to_string(), "artist_id".to_string())],
        "an artist is kept by the tracks crediting it; `work_artists` declares the \
         artist as its parent, so it is the back-edge there",
    );
}

#[test]
fn a_managed_release_publishes_the_work_graph_it_declares() {
    let c = conn();
    create_work_graph_schema(&c);
    seed_work_graph(&c, 0);
    let tables = work_graph_tables();

    let bytes = super::tests::capture_and_gate(
        &c,
        &tables,
        &["UPDATE releases SET managed = 1, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'"],
    );
    let changes = crate::walk_changeset(&bytes).expect("walk gated changeset");
    let published = |table: &str, pk: &str| {
        changes
            .iter()
            .any(|c| c.table == table && c.pk() == Some(pk))
    };

    for (table, pk) in [
        ("releases", "R"),
        ("tracks", "T"),
        ("track_works", "TW"),
        ("track_artists", "TA"),
        ("works", "W_PART"),
        ("artists", "AR"),
        ("work_artists", "WA"),
        ("work_parts", "WP"),
    ] {
        assert!(published(table, pk), "{table}.{pk} is published");
    }
    assert!(
        published("works", "W_WHOLE"),
        "the container work comes along by closure: the published `work_parts` row \
         names it, and no keep-relation would ever bring it",
    );

    let peer = conn();
    create_work_graph_schema(&peer);
    peer.apply_test_changeset(&bytes, &tables);
    assert_eq!(
        peer.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("foreign-key check"),
        0,
        "the published set resolves every foreign key it carries",
    );
}

#[test]
fn the_snapshot_prune_follows_the_same_declarations() {
    for (managed, kept) in [(1u8, true), (0, false)] {
        let c = conn();
        create_work_graph_schema(&c);
        seed_work_graph(&c, managed);

        Gates::from_tables(&c, &work_graph_tables())
            .expect("gates")
            .delete_gated_false(&c)
            .expect("prune the snapshot copy");

        for (table, pk) in [
            ("work_parts", "WP"),
            ("works", "W_WHOLE"),
            ("works", "W_PART"),
            ("work_artists", "WA"),
            ("artists", "AR"),
        ] {
            assert_eq!(
                c.test_row_exists(&format!("SELECT 1 FROM {table} WHERE id = '{pk}'")),
                kept,
                "{table}.{pk} survives the prune iff the release is managed",
            );
        }
    }
}

#[test]
fn a_container_work_survives_while_another_shared_part_still_names_it() {
    let c = conn();
    create_work_graph_schema(&c);
    c.execute_test_sql(
        "INSERT INTO releases (id, managed, _updated_at) VALUES ('R1', 1, '0000000001000-0000-dev1');
         INSERT INTO releases (id, managed, _updated_at) VALUES ('R2', 1, '0000000001000-0000-dev1');
         INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T1', 'R1', '0000000001000-0000-dev1');
         INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T2', 'R2', '0000000001000-0000-dev1');
         INSERT INTO works (id, title, _updated_at) VALUES ('W_PART1', 'Part One', '0000000001000-0000-dev1');
         INSERT INTO works (id, title, _updated_at) VALUES ('W_PART2', 'Part Two', '0000000001000-0000-dev1');
         INSERT INTO works (id, title, _updated_at) VALUES ('W_WHOLE', 'The Real Book', '0000000001000-0000-dev1');
         INSERT INTO artists (id, name, _updated_at) VALUES ('AR', 'Composer', '0000000001000-0000-dev1');
         INSERT INTO track_works (id, track_id, work_id, _updated_at) VALUES ('TW1', 'T1', 'W_PART1', '0000000001000-0000-dev1');
         INSERT INTO track_works (id, track_id, work_id, _updated_at) VALUES ('TW2', 'T2', 'W_PART2', '0000000001000-0000-dev1');
         INSERT INTO track_artists (id, track_id, artist_id, _updated_at) VALUES ('TA1', 'T1', 'AR', '0000000001000-0000-dev1');
         INSERT INTO track_artists (id, track_id, artist_id, _updated_at) VALUES ('TA2', 'T2', 'AR', '0000000001000-0000-dev1');
         INSERT INTO work_parts (id, parent_work_id, child_work_id, _updated_at) VALUES ('WP1', 'W_WHOLE', 'W_PART1', '0000000001000-0000-dev1');
         INSERT INTO work_parts (id, parent_work_id, child_work_id, _updated_at) VALUES ('WP2', 'W_WHOLE', 'W_PART2', '0000000001000-0000-dev1');",
    );

    let bytes = super::tests::capture_and_gate(
        &c,
        &work_graph_tables(),
        &[
            "UPDATE releases SET managed = 0, _updated_at = '0000000002000-0000-dev1' WHERE id = 'R1'",
        ],
    );
    let changes = crate::walk_changeset(&bytes).expect("walk gated changeset");
    let deleted = |table: &str, pk: &str| {
        changes.iter().any(|c| {
            c.table == table
                && c.pk() == Some(pk)
                && c.op == coven_foundation::changeset::ChangeOp::Delete
        })
    };

    assert!(
        deleted("work_parts", "WP1"),
        "the retracted join row leaves"
    );
    assert!(
        deleted("works", "W_PART1"),
        "the work only the retracted track recorded leaves"
    );
    assert!(
        !deleted("works", "W_WHOLE"),
        "the container work stays: WP2 is still shared and still names it",
    );
    assert!(!deleted("work_parts", "WP2"));
    assert!(
        !deleted("artists", "AR"),
        "the artist is still credited on the surviving release's track"
    );
}

/// A track's audio formats and their segments: a chain of plain tables, each
/// naming the foreign key above it.
fn audio_format_tables() -> Vec<SyncedTable> {
    vec![
        plain("releases").gated_by("managed"),
        plain("tracks").inherits_audience_through("release_id"),
        plain("audio_formats").inherits_audience_through("track_id"),
        plain("audio_format_segments").inherits_audience_through("audio_format_id"),
    ]
}

fn create_audio_format_schema(c: &Connection) {
    c.execute_test_sql(
        "CREATE TABLE releases (
                id TEXT PRIMARY KEY,
                managed INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE tracks (
                id TEXT PRIMARY KEY,
                release_id TEXT NOT NULL REFERENCES releases (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE audio_formats (
                id TEXT PRIMARY KEY,
                track_id TEXT NOT NULL REFERENCES tracks (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;
         CREATE TABLE audio_format_segments (
                id TEXT PRIMARY KEY,
                audio_format_id TEXT NOT NULL REFERENCES audio_formats (id) ON DELETE CASCADE,
                _updated_at TEXT NOT NULL
             ) STRICT;",
    );
}

#[test]
fn a_gate_flip_reaches_the_end_of_a_plain_table_chain() {
    let c = conn();
    create_audio_format_schema(&c);
    let tables = audio_format_tables();
    c.execute_test_sql(
        "INSERT INTO releases (id, managed, _updated_at) VALUES ('R', 0, '0000000001000-0000-dev1');
         INSERT INTO tracks (id, release_id, _updated_at) VALUES ('T', 'R', '0000000001000-0000-dev1');
         INSERT INTO audio_formats (id, track_id, _updated_at) VALUES ('F', 'T', '0000000001000-0000-dev1');
         INSERT INTO audio_format_segments (id, audio_format_id, _updated_at) VALUES ('S', 'F', '0000000001000-0000-dev1');",
    );

    let private = super::tests::capture_and_gate(
        &c,
        &tables,
        &["UPDATE releases SET _updated_at = '0000000002000-0000-dev1' WHERE id = 'R'"],
    );
    assert!(
        crate::walk_changeset(&private).expect("walk").is_empty(),
        "nothing under an unmanaged release is published"
    );

    let shared = super::tests::capture_and_gate(
        &c,
        &tables,
        &["UPDATE releases SET managed = 1, _updated_at = '0000000003000-0000-dev1' WHERE id = 'R'"],
    );
    let changes = crate::walk_changeset(&shared).expect("walk");
    for (table, pk) in [
        ("releases", "R"),
        ("tracks", "T"),
        ("audio_formats", "F"),
        ("audio_format_segments", "S"),
    ] {
        assert!(
            changes
                .iter()
                .any(|c| c.table == table && c.pk() == Some(pk)),
            "{table}.{pk} is published once the release is managed",
        );
    }
}
