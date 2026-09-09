use super::*;

#[path = "replay_projection_rowid_tests.rs"]
mod rowids;

fn relationship_database(schema: &str) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().expect("open relationship database");
    connection
        .execute_batch("PRAGMA foreign_keys = ON")
        .expect("enable foreign keys");
    connection
        .execute_batch(schema)
        .expect("create relationship database");
    connection
}

#[test]
fn projection_install_does_not_read_unchanged_local_row_identities() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY,
            code TEXT COLLATE NOCASE UNIQUE NOT NULL, content TEXT);
        CREATE TABLE local_links (rowid TEXT, _rowid_ TEXT, oid TEXT,
            parent TEXT REFERENCES parents(code) ON UPDATE CASCADE ON DELETE CASCADE);
        INSERT INTO parents VALUES ('parent', 'ALPHA', 'before');
        INSERT INTO local_links VALUES ('one', 'two', 'three', 'alpha');";
    let source = relationship_database(schema);
    source
        .execute("UPDATE parents SET code = 'alpha', content = 'after'", [])
        .expect("change only collation-equal key and ordinary content");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("unchanged relationships require no local row identity");
    assert_eq!(
        tx.query_row(
            "SELECT rowid, _rowid_, oid, parent FROM local_links",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            }
        )
        .expect("read unchanged local values"),
        ("one".into(), "two".into(), "three".into(), "alpha".into())
    );
    tx.commit().expect("commit unrelated parent change");
}

#[test]
fn projection_install_propagates_coupled_unique_swaps_through_local_keys() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (
            code TEXT PRIMARY KEY REFERENCES parents(code) ON UPDATE CASCADE ON DELETE CASCADE,
            content TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE local_details (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL REFERENCES local_links(code) ON UPDATE CASCADE ON DELETE CASCADE
        );
        INSERT INTO parents VALUES ('one', 'A'), ('two', 'B');
        INSERT INTO local_links VALUES ('A', 'first'), ('B', 'second');
        INSERT INTO local_details VALUES (17, 'A'), (29, 'B');";
    let source = relationship_database(schema);
    source
        .execute_batch(
            "UPDATE parents SET code = 'temporary' WHERE id = 'one';
        UPDATE parents SET code = 'A' WHERE id = 'two';
        UPDATE parents SET code = 'B' WHERE id = 'one';",
        )
        .expect("swap source keys");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install coupled swap");
    assert_eq!(
        crate::query_mapped_rows(
            &tx,
            "SELECT code, content FROM local_links ORDER BY content",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        )
        .expect("read local links"),
        [("B".into(), "first".into()), ("A".into(), "second".into())]
    );
    assert_eq!(
        crate::query_mapped_rows(
            &tx,
            "SELECT id, code FROM local_details ORDER BY id",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        )
        .expect("read descendants"),
        [(17, "B".into()), (29, "A".into())]
    );
    tx.commit().expect("commit coupled swap");
}

#[test]
fn projection_install_cascades_deletions_through_current_local_descendants() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY,
            parent TEXT REFERENCES parents(id) ON DELETE CASCADE);
        CREATE TABLE local_details (description TEXT,
            link INTEGER REFERENCES local_links(id) ON DELETE CASCADE);
        INSERT INTO parents VALUES ('removed'), ('retained');";
    let source = relationship_database(schema);
    source
        .execute("DELETE FROM parents WHERE id = 'removed'", [])
        .expect("delete source parent");
    let mut target = relationship_database(schema);
    target
        .execute_batch(
            "INSERT INTO local_links VALUES (4, 'removed'), (8, 'retained');
        INSERT INTO local_details(rowid, description, link) VALUES
        (101, 'remove', 4), (202, 'keep', 8);",
        )
        .expect("add current device-local descendants");
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()]).expect("install deletion");
    assert_eq!(
        crate::query_mapped_rows(
            &tx,
            "SELECT rowid, description, link FROM local_details",
            [],
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?
            ))
        )
        .expect("read retained row identity"),
        [(202, "keep".into(), 8)]
    );
    tx.commit().expect("commit deletion");
}

#[test]
fn projection_install_matches_composite_parent_keys_with_native_affinity_and_collation() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, number INTEGER NOT NULL,
            code TEXT COLLATE NOCASE NOT NULL, UNIQUE(number, code));
        CREATE TABLE local_links (id TEXT PRIMARY KEY, number TEXT NOT NULL, code TEXT NOT NULL,
            FOREIGN KEY(number, code) REFERENCES parents(number, code) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('parent', 1, 'ALPHA');
        INSERT INTO local_links VALUES ('local', '01', 'alpha');";
    let source = relationship_database(schema);
    source
        .execute("DELETE FROM local_links", [])
        .expect("source has no device-local rows");
    source
        .execute("UPDATE parents SET number = 2, code = 'BETA'", [])
        .expect("update composite key");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install composite update");
    let values: (String, String, String) = tx
        .query_row(
            "SELECT number, typeof(number), code FROM local_links",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read coerced local key");
    assert_eq!(values, ("2".into(), "text".into(), "BETA".into()));
    tx.commit().expect("commit composite update");
}

#[test]
fn projection_install_matches_children_using_only_parent_affinity() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY, code INTEGER
            REFERENCES parents(code) ON DELETE CASCADE);
        INSERT INTO parents VALUES ('removed', '01'), ('retained', '1');
        INSERT INTO local_links VALUES (17, 1);";
    let source = relationship_database(schema);
    source
        .execute_batch("DELETE FROM local_links; DELETE FROM parents WHERE id = 'removed';")
        .expect("prepare accepted parent transition without device-local rows");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("preserve the child of the other text parent");
    assert_eq!(
        tx.query_row(
            "SELECT COUNT(*) FROM local_links WHERE id = 17 AND code = 1",
            [],
            |row| row.get::<_, i64>(0)
        )
        .expect("read preserved local relationship"),
        1
    );
    tx.commit().expect("commit exact parent matching");
}

#[test]
fn projection_install_evaluates_composite_set_defaults_on_real_deletion() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, number INTEGER NOT NULL,
            code TEXT COLLATE NOCASE NOT NULL, UNIQUE(number, code));
        CREATE TABLE local_links (id TEXT PRIMARY KEY,
            number TEXT DEFAULT (1 + 2), code TEXT DEFAULT ('fall' || 'back'),
            FOREIGN KEY(number, code) REFERENCES parents(number, code) ON DELETE SET DEFAULT);
        INSERT INTO parents VALUES ('removed', 1, 'ALPHA'), ('fallback', 3, 'FALLBACK');
        INSERT INTO local_links VALUES ('local', '01', 'alpha');";
    let source = relationship_database(schema);
    source
        .execute("DELETE FROM local_links", [])
        .expect("source has no device-local rows");
    source
        .execute("DELETE FROM parents WHERE id = 'removed'", [])
        .expect("delete source parent");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install default action");
    let values: (String, String) = tx
        .query_row("SELECT number, code FROM local_links", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("read default values");
    assert_eq!(values, ("3".into(), "fallback".into()));
    tx.commit().expect("commit default action");
}

#[test]
fn projection_install_keeps_receiver_context_for_recursive_defaults() {
    install_recursive_contextual_default("last_insert_rowid()");
}

#[test]
fn projection_install_keeps_receiver_change_count_for_recursive_defaults() {
    install_recursive_contextual_default("changes() + 89");
}

#[test]
fn projection_install_keeps_receiver_total_changes_for_recursive_defaults() {
    install_recursive_contextual_default("total_changes() + 84");
}

fn install_recursive_contextual_default(expression: &str) {
    let schema = format!(
        "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY, code TEXT UNIQUE
            REFERENCES parents(code) ON UPDATE CASCADE);
        CREATE TABLE local_details (id INTEGER PRIMARY KEY, link TEXT
            DEFAULT ({expression}) REFERENCES local_links(code) ON UPDATE SET DEFAULT);
        CREATE TABLE local_context (id INTEGER PRIMARY KEY);
        INSERT INTO parents VALUES ('changed', 'A'), ('fallback', '91');
        INSERT INTO local_links VALUES (4, 'A'), (17, '91');
        INSERT INTO local_details VALUES (91, 'A');
        INSERT INTO local_context VALUES (90), (91);"
    );
    let source = relationship_database(&schema);
    source
        .execute_batch(
            "DELETE FROM local_details; DELETE FROM local_links;
        UPDATE parents SET code = 'B' WHERE id = 'changed';",
        )
        .expect("prepare accepted parent transition without local rows");
    let mut target = relationship_database(&schema);
    assert_eq!(
        target
            .query_row(&format!("SELECT {expression}"), [], |row| row
                .get::<_, i64>(0))
            .expect("evaluate initial receiving context"),
        91,
        "receiving connection context"
    );
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("evaluate recursive defaults without evaluator writes changing their context");
    assert_eq!(
        tx.query_row("SELECT link FROM local_details WHERE id = 91", [], |row| {
            row.get::<_, String>(0)
        })
        .expect("read receiving default"),
        "91"
    );
    assert!(!tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get::<_, bool>(0)
        )
        .expect("validate final local graph"));
    tx.commit().expect("commit recursive receiving default");
}

#[test]
fn projection_install_rolls_back_when_a_local_cascade_violates_its_schema() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (id TEXT PRIMARY KEY,
            code TEXT CHECK(code != 'blocked') REFERENCES parents(code) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('parent', 'original');";
    let source = relationship_database(schema);
    source
        .execute("UPDATE parents SET code = 'blocked'", [])
        .expect("update source without local rows");
    let mut target = relationship_database(schema);
    target
        .execute("INSERT INTO local_links VALUES ('local', 'original')", [])
        .expect("insert local row");
    {
        let tx = target.transaction().expect("begin installation");
        replace_tables_from_connection_on(&source, &tx, &["parents".into()])
            .expect_err("local CHECK constraint must reject the complete installation");
    }
    assert_eq!(
        target
            .query_row("SELECT code FROM parents", [], |row| row
                .get::<_, String>(0))
            .expect("read rolled back parent"),
        "original"
    );
    assert_eq!(
        target
            .query_row("SELECT code FROM local_links", [], |row| row
                .get::<_, String>(0))
            .expect("read rolled back local child"),
        "original"
    );
}

#[test]
fn projection_install_propagates_generated_local_parent_keys() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY,
            code TEXT REFERENCES parents(code) ON UPDATE CASCADE,
            derived TEXT GENERATED ALWAYS AS (lower(code) || ':detail') STORED UNIQUE);
        CREATE TABLE local_details (id INTEGER PRIMARY KEY,
            link TEXT REFERENCES local_links(derived) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('parent', 'ALPHA');";
    let source = relationship_database(schema);
    source
        .execute("UPDATE parents SET code = 'BETA'", [])
        .expect("change parent key");
    let mut target = relationship_database(schema);
    target
        .execute_batch(
            "INSERT INTO local_links(id, code) VALUES (17, 'ALPHA');
        INSERT INTO local_details VALUES (29, 'alpha:detail');",
        )
        .expect("insert current generated relationship");
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install generated key transition");
    assert_eq!(
        tx.query_row(
            "SELECT code, derived FROM local_links WHERE id = 17",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        )
        .expect("read generated values"),
        ("BETA".into(), "beta:detail".into())
    );
    assert_eq!(
        tx.query_row("SELECT link FROM local_details WHERE id = 29", [], |row| {
            row.get::<_, String>(0)
        })
        .expect("read generated descendant key"),
        "beta:detail"
    );
    tx.commit().expect("commit generated relationship");
}

#[test]
fn projection_install_combines_compatible_actions_after_column_affinity() {
    let schema = "CREATE TABLE numeric_parents (id TEXT PRIMARY KEY, code INTEGER UNIQUE NOT NULL);
        CREATE TABLE text_parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY,
            code TEXT REFERENCES numeric_parents(code) ON UPDATE CASCADE
                      REFERENCES text_parents(code) ON UPDATE CASCADE);
        INSERT INTO numeric_parents VALUES ('parent', 1);
        INSERT INTO text_parents VALUES ('parent', '1');";
    let source = relationship_database(schema);
    source
        .execute_batch("UPDATE numeric_parents SET code = 2; UPDATE text_parents SET code = '2';")
        .expect("change both parent keys");
    let mut target = relationship_database(schema);
    target
        .execute("INSERT INTO local_links VALUES (9, '1')", [])
        .expect("insert shared local reference");
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(
        &source,
        &tx,
        &["numeric_parents".into(), "text_parents".into()],
    )
    .expect("combine matching native-affinity results");
    assert_eq!(
        tx.query_row("SELECT code, typeof(code) FROM local_links", [], |row| Ok(
            (row.get::<_, String>(0)?, row.get::<_, String>(1)?)
        ))
        .expect("read combined value"),
        ("2".into(), "text".into())
    );
    tx.commit().expect("commit combined actions");
}

#[test]
fn projection_install_restricts_live_children_and_honors_explicit_deferral() {
    for deferred in [false, true] {
        let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
            CREATE TABLE local_links (id INTEGER PRIMARY KEY,
                code TEXT REFERENCES parents(code) ON UPDATE RESTRICT);
            INSERT INTO parents VALUES ('one', 'A'), ('two', 'B');";
        let source = relationship_database(schema);
        source
            .execute_batch(
                "UPDATE parents SET code = 'temporary' WHERE id = 'one';
            UPDATE parents SET code = 'A' WHERE id = 'two';
            UPDATE parents SET code = 'B' WHERE id = 'one';",
            )
            .expect("swap source keys without local rows");
        let mut target = relationship_database(schema);
        target
            .execute("INSERT INTO local_links VALUES (9, 'A')", [])
            .expect("insert current local reference");
        let tx = target.transaction().expect("begin installation");
        tx.pragma_update(None, "defer_foreign_keys", deferred)
            .expect("choose transaction deferral");
        let result = replace_tables_from_connection_on(&source, &tx, &["parents".into()]);
        if deferred {
            result.expect("deferred restriction validates the final relationship");
            assert_eq!(
                tx.query_row("SELECT code FROM local_links", [], |row| row
                    .get::<_, String>(0))
                    .expect("read unmodified reference"),
                "A"
            );
            assert!(!tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                    [],
                    |row| row.get::<_, bool>(0)
                )
                .expect("validate deferred relationship"));
            tx.commit().expect("commit deferred relationship");
        } else {
            result.expect_err("immediate restriction refuses surviving local child");
            drop(tx);
            assert_eq!(
                target
                    .query_row("SELECT code FROM parents WHERE id = 'one'", [], |row| {
                        row.get::<_, String>(0)
                    })
                    .expect("read rolled back parent"),
                "A"
            );
        }
    }
}

#[test]
fn projection_install_sets_null_and_preserves_collation_equal_keys() {
    let schema =
        "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT COLLATE NOCASE UNIQUE NOT NULL);
        CREATE TABLE nullable_links (id INTEGER PRIMARY KEY,
            code TEXT REFERENCES parents(code) ON UPDATE SET NULL);
        CREATE TABLE local_links (id INTEGER PRIMARY KEY,
            code TEXT REFERENCES parents(code) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('changed', 'ALPHA'), ('equal', 'BETA');";
    let source = relationship_database(schema);
    source
        .execute_batch(
            "UPDATE parents SET code = 'GAMMA' WHERE id = 'changed';
        UPDATE parents SET code = 'beta' WHERE id = 'equal';",
        )
        .expect("change parent keys");
    let mut target = relationship_database(schema);
    target
        .execute_batch(
            "INSERT INTO nullable_links VALUES (4, 'ALPHA');
        INSERT INTO local_links VALUES (8, 'BETA');",
        )
        .expect("insert local relationships");
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install native key comparisons");
    assert_eq!(
        tx.query_row("SELECT code FROM nullable_links", [], |row| row
            .get::<_, Option<String>>(0))
            .expect("read SET NULL result"),
        None
    );
    assert_eq!(
        tx.query_row("SELECT code FROM local_links", [], |row| row
            .get::<_, String>(0))
            .expect("read collation-equal relationship"),
        "BETA"
    );
    tx.commit().expect("commit local actions");
}
