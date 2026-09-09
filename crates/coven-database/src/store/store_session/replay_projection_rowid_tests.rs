use super::*;

fn shadowed_rowid_database() -> rusqlite::Connection {
    relationship_database(
        "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (visible_rowid TEXT, _rowid_ TEXT, oid TEXT,
            code TEXT UNIQUE REFERENCES parents(code) ON UPDATE CASCADE ON DELETE CASCADE);
        CREATE INDEX local_content ON local_links(visible_rowid);
        CREATE VIEW local_view AS SELECT visible_rowid, code FROM local_links;
        CREATE TABLE local_audit(content TEXT);
        CREATE TRIGGER local_update AFTER UPDATE ON local_links BEGIN
            INSERT INTO local_audit VALUES(new.visible_rowid);
        END;
        INSERT INTO parents VALUES ('one', 'A'), ('two', 'B');
        INSERT INTO local_links(rowid, visible_rowid, _rowid_, oid, code)
            VALUES (17, 'first', 'shadow-a', 'shadow-b', 'A'),
                   (29, 'second', 'shadow-c', 'shadow-d', 'B');
        ALTER TABLE local_links RENAME COLUMN visible_rowid TO rowid;",
    )
}

fn schema_definitions(connection: &rusqlite::Connection) -> Vec<(String, String, Option<String>)> {
    crate::query_mapped_rows(
        connection,
        "SELECT type, name, sql FROM main.sqlite_schema ORDER BY type, name",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .expect("read exact host schema")
}

#[test]
fn projection_install_preserves_shadowed_rowids_and_host_schema_during_key_swaps() {
    let source = shadowed_rowid_database();
    source
        .execute_batch(
            "DELETE FROM local_links;
        UPDATE parents SET code = 'temporary' WHERE id = 'one';
        UPDATE parents SET code = 'A' WHERE id = 'two';
        UPDATE parents SET code = 'B' WHERE id = 'one';",
        )
        .expect("prepare accepted parent transition without local rows");
    let mut target = shadowed_rowid_database();
    let schema = schema_definitions(&target);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install actual changes to rows with hidden physical identity");
    assert_eq!(schema_definitions(&tx), schema);
    assert_eq!(
        crate::query_mapped_rows(
            &tx,
            "SELECT rowid, _rowid_, oid, code FROM local_links ORDER BY rowid",
            [],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?
            ))
        )
        .expect("read visible local fields"),
        [
            (
                "first".into(),
                "shadow-a".into(),
                "shadow-b".into(),
                "B".into()
            ),
            (
                "second".into(),
                "shadow-c".into(),
                "shadow-d".into(),
                "A".into()
            ),
        ]
    );
    tx.commit().expect("commit local identity transition");

    let inspection = target
        .transaction()
        .expect("inspect otherwise hidden rowids");
    inspection
        .execute_batch("ALTER TABLE local_links RENAME COLUMN rowid TO visible_rowid")
        .expect("expose hidden identity for inspection");
    assert_eq!(
        crate::query_mapped_rows(
            &inspection,
            "SELECT rowid, visible_rowid, code FROM local_links ORDER BY rowid",
            [],
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?
            ))
        )
        .expect("read physical identities"),
        [
            (17, "first".into(), "B".into()),
            (29, "second".into(), "A".into())
        ]
    );
    inspection.rollback().expect("restore inspection schema");
    assert_eq!(schema_definitions(&target), schema);
}

#[test]
fn projection_install_restores_shadowed_rowid_schema_after_a_constraint_failure() {
    let source = shadowed_rowid_database();
    source
        .execute_batch(
            "DELETE FROM local_links;
        UPDATE parents SET code = 'A1' WHERE id = 'one';
        UPDATE parents SET code = 'A2' WHERE id = 'two';",
        )
        .expect("prepare parents whose local children cannot both satisfy their index");
    let mut target = shadowed_rowid_database();
    target
        .execute_batch("CREATE UNIQUE INDEX local_code_class ON local_links(substr(code, 1, 1))")
        .expect("add native local expression constraint");
    let schema = schema_definitions(&target);
    {
        let tx = target.transaction().expect("begin installation");
        let error = replace_tables_from_connection_on(&source, &tx, &["parents".into()])
            .expect_err("the complete local projection violates its actual expression index");
        assert!(
            error.to_string().contains("UNIQUE constraint failed"),
            "{error}"
        );
        assert_eq!(
            schema_definitions(&tx),
            schema,
            "scoped names are restored before returning the error"
        );
    }
    assert_eq!(schema_definitions(&target), schema);
    assert_eq!(
        crate::query_mapped_rows(
            &target,
            "SELECT id, code FROM parents ORDER BY id",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        )
        .expect("read parents after rollback"),
        [("one".into(), "A".into()), ("two".into(), "B".into())]
    );
    let inspection = target
        .transaction()
        .expect("inspect hidden rowids after rollback");
    inspection
        .execute_batch("ALTER TABLE local_links RENAME COLUMN rowid TO visible_rowid")
        .expect("expose hidden identity for inspection");
    assert_eq!(
        crate::query_mapped_rows(
            &inspection,
            "SELECT rowid, visible_rowid, code FROM local_links ORDER BY rowid",
            [],
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?
            ))
        )
        .expect("read physical identities after rollback"),
        [
            (17, "first".into(), "A".into()),
            (29, "second".into(), "B".into())
        ]
    );
    inspection.rollback().expect("restore inspection schema");
}

#[test]
fn projection_install_restores_quoted_local_column_dependencies() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL);
        CREATE TABLE local_links (_rowid_ TEXT, oid TEXT,
            code TEXT UNIQUE REFERENCES parents(code) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('one', 'A');
        INSERT INTO local_links(rowid, _rowid_, oid, code) VALUES(17, 'shadow-a', 'shadow-b', 'A');
        ALTER TABLE local_links ADD COLUMN [rowid] TEXT;
        UPDATE local_links SET [rowid] = 'content';
        CREATE INDEX local_content ON local_links([rowid]);
        CREATE VIEW local_view AS SELECT `rowid`, code FROM local_links;
        CREATE TABLE local_audit(content TEXT);
        CREATE TRIGGER local_update AFTER UPDATE ON local_links BEGIN
            INSERT INTO local_audit VALUES(new.[rowid]);
        END;";
    let source = relationship_database(schema);
    source
        .execute_batch("DELETE FROM local_links; UPDATE parents SET code = 'B';")
        .expect("prepare parent transition without device-local rows");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("install with bracket and backtick dependent identifiers");
    assert_eq!(
        tx.query_row(
            "SELECT [rowid], code FROM local_links INDEXED BY local_content",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        )
        .expect("read restored indexed column"),
        ("content".into(), "B".into())
    );
    assert_eq!(
        tx.query_row("SELECT `rowid`, code FROM local_view", [], |row| Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?
        )))
        .expect("read restored view"),
        ("content".into(), "B".into())
    );
    tx.execute("UPDATE local_links SET [rowid] = 'after'", [])
        .expect("host update uses its restored trigger");
    assert_eq!(
        tx.query_row(
            "SELECT content FROM local_audit ORDER BY rowid DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0)
        )
        .expect("read restored trigger result"),
        "after"
    );
    tx.commit().expect("commit restored quoted schema");
    let inspection = target.transaction().expect("inspect physical identity");
    inspection
        .execute_batch("ALTER TABLE local_links RENAME COLUMN [rowid] TO visible_rowid")
        .expect("expose hidden rowid for inspection");
    assert_eq!(
        inspection
            .query_row("SELECT rowid FROM local_links", [], |row| row
                .get::<_, i64>(0))
            .expect("read preserved physical identity"),
        17
    );
    inspection.rollback().expect("restore inspection schema");
}

#[test]
fn projection_install_follows_relationships_through_an_exposed_column() {
    let schema = "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE);
        CREATE TABLE local_links (visible_rowid TEXT UNIQUE REFERENCES parents(code)
            ON UPDATE CASCADE, _rowid_ TEXT, oid TEXT);
        CREATE TABLE local_children (id INTEGER PRIMARY KEY, parent TEXT
            REFERENCES local_links(visible_rowid) ON UPDATE CASCADE);
        INSERT INTO parents VALUES ('one', 'A');
        INSERT INTO local_links(rowid, visible_rowid, _rowid_, oid)
            VALUES (17, 'A', 'shadow-a', 'shadow-b');
        INSERT INTO local_children VALUES (29, 'A');
        ALTER TABLE local_links RENAME COLUMN visible_rowid TO rowid;";
    let source = relationship_database(schema);
    source
        .execute_batch(
            "DELETE FROM local_children; DELETE FROM local_links;
            UPDATE parents SET code = 'B';",
        )
        .expect("prepare accepted parent transition");
    let mut target = relationship_database(schema);
    let tx = target.transaction().expect("begin installation");
    replace_tables_from_connection_on(&source, &tx, &["parents".into()])
        .expect("follow both sides of the renamed relationship");
    assert_eq!(
        tx.query_row(
            "SELECT parent FROM local_children WHERE id = 29",
            [],
            |row| row.get::<_, String>(0)
        )
        .expect("read descendant"),
        "B"
    );
    assert_eq!(
        tx.query_row("SELECT rowid FROM local_links", [], |row| row
            .get::<_, String>(0))
            .expect("read restored parent column"),
        "B"
    );
    assert_eq!(
        tx.query_row(
            "SELECT COUNT(*) FROM sqlite_schema
        WHERE sql LIKE '%coven_projection_rowid_%'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .expect("check exposure restoration"),
        0
    );
    tx.commit()
        .expect("commit recursive relationship transition");
}
