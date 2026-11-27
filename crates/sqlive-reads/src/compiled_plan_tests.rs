use super::CompiledPlan;
use crate::read_capture::WholeViews;
use crate::{MainSchema, SchemaProbe, StandInFunction};
use rusqlite::Connection;

const SCHEMA: &str = "
    CREATE TABLE releases (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, year REAL);
    CREATE TABLE tracks (id TEXT NOT NULL PRIMARY KEY, release_id TEXT NOT NULL, title TEXT);
    CREATE VIEW titled AS SELECT id, title FROM releases WHERE title <> '';";

const TRACKS_OF: &str = "SELECT t.title FROM tracks AS t JOIN releases AS r ON r.id = t.release_id
     WHERE r.id = :release AND r.year < 1e999";

fn probe(schema: &str, whole: &[&str]) -> SchemaProbe {
    let whole: Vec<String> = whole.iter().map(|name| (*name).to_owned()).collect();
    let functions = [StandInFunction { name: "fold".into(), arguments: 1, deterministic: true }];
    SchemaProbe::open(&[schema.to_owned()], &functions, &[], &whole).unwrap()
}

fn compiled(probe: &SchemaProbe, sql: &str) -> CompiledPlan {
    probe.compiled_plan(sql).unwrap().unwrap()
}

/// A database of `schema` with `fold` defined as `deterministic`.
fn database(schema: &str, deterministic: bool) -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    let mut flags = rusqlite::functions::FunctionFlags::SQLITE_UTF8;
    if deterministic {
        flags |= rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC;
    }
    connection.create_scalar_function("fold", 1, flags, |_| Ok(None::<i64>)).unwrap();
    connection.execute_batch(schema).unwrap();
    connection
}

/// What `plan`, read back from its text, finds different in `connection`,
/// where a live query reads `whole` whole.
fn differences(plan: &CompiledPlan, connection: &Connection, whole: &[&str]) -> Vec<String> {
    let decoded = CompiledPlan::decode(&plan.encode()).unwrap();
    let schema = MainSchema::read(connection).unwrap();
    match decoded.check(connection, &schema, &WholeViews::new(whole.iter().copied())).unwrap() {
        Ok(_) => Vec::new(),
        Err(super::SchemaDiffers { differ }) => differ,
    }
}

/// A plan read back from its text narrows as the plan built: its constants,
/// an infinity among them, its join and its parameter.
#[test]
fn a_plan_read_back_from_its_text_narrows_as_it_did() {
    let probe = probe(SCHEMA, &[]);
    let plan = compiled(&probe, TRACKS_OF);
    let decoded = CompiledPlan::decode(&plan.encode()).unwrap();
    assert_eq!(decoded.report(), plan.report());
    assert_eq!(decoded.report().to_string(), "releases: keyed by id\ntracks: keyed by release_id");
    let connection = database(SCHEMA, true);
    let schema = MainSchema::read(&connection).unwrap();
    let checked = decoded.check(&connection, &schema, &WholeViews::new([])).unwrap();
    assert!(checked.is_ok());
}

/// A table or view defined otherwise, a unique index added, or a view the
/// host reads whole now differs, each by its name; an index that keeps no
/// value unique doesn't, since no plan reads it.
#[test]
fn a_schema_that_defines_an_object_otherwise_differs_by_its_name() {
    let probe = probe(SCHEMA, &[]);
    let plan = compiled(&probe, TRACKS_OF);
    let changed = SCHEMA.replace("title TEXT)", "title TEXT, position INTEGER)");
    assert_eq!(differences(&plan, &database(&changed, true), &[]), ["table tracks"]);
    let unique = format!("{SCHEMA} CREATE UNIQUE INDEX titles ON releases (title);");
    assert_eq!(differences(&plan, &database(&unique, true), &[]), ["table releases"]);
    let index = format!("{SCHEMA} CREATE INDEX titles ON releases (title);");
    assert!(differences(&plan, &database(&index, true), &[]).is_empty());

    let titled = compiled(&probe, "SELECT title FROM titled WHERE id = :id");
    let view = SCHEMA.replace("title <> ''", "title <> 'untitled'");
    assert_eq!(differences(&titled, &database(&view, true), &[]), ["view titled"]);
    assert_eq!(differences(&titled, &database(SCHEMA, true), &["titled"]), ["view titled"]);
}

/// A function the plan asked about, defined otherwise, differs: whether it
/// repeats decides whether a condition on its value narrows.
#[test]
fn a_function_defined_otherwise_differs() {
    let probe = probe(SCHEMA, &[]);
    let plan = compiled(&probe, "SELECT id FROM releases WHERE fold(title) = :title");
    assert!(differences(&plan, &database(SCHEMA, true), &[]).is_empty());
    assert_eq!(differences(&plan, &database(SCHEMA, false), &[]), ["function fold"]);
}

/// A statement sqlive can't analyze has no compiled plan: neither one
/// whose shape keeps every table whole, nor one that counts every column of
/// the schema it was built against as read, as a view the parser can't read
/// makes it.
#[test]
fn a_statement_sqlive_cant_analyze_has_no_plan() {
    let probe = probe(SCHEMA, &[]);
    let why = probe.compiled_plan("SELECT title FROM tracks NATURAL JOIN releases").unwrap();
    assert_eq!(why.err().unwrap().why, "a NATURAL or RIGHT join");
    let unparsed = format!("{SCHEMA} CREATE VIEW unparsed AS SELECT id FROM releases /* open");
    let probe = SchemaProbe::open(&[unparsed], &[], &[], &["unparsed".to_owned()]).unwrap();
    let why = probe.compiled_plan("SELECT id FROM unparsed").unwrap().err().unwrap().why;
    assert!(why.contains("unparsed"), "{why}");
}
