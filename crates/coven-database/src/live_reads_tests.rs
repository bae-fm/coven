use super::{Both, ReadAs};
use crate::tests::{next_result, TestStore};
use crate::write::tests::{notes, NOTES};
use crate::{DbError, Writer};
use sqlive::rusqlite::{Row, ToSql};
use sqlive::CompiledStatement;
use sqlive_compose::{Read, Returning, Statement};
use std::sync::OnceLock;

/// `sql` with the scope plan built against `NOTES`, as an app's build
/// compiles it.
fn compiled(sql: &'static str) -> CompiledStatement<Writer> {
    let probe = sqlive::SchemaProbe::open(&[NOTES.to_owned()], &[], &[]).unwrap();
    let plan = probe.compiled_plan(sql).unwrap().unwrap().encode();
    CompiledStatement::new(sql, Box::leak(plan.into_boxed_str()))
}

/// The ids of the notes titled `title`, as an app's build compiles the read.
struct Titled {
    title: String,
}

impl Statement for Titled {
    const SQL: &'static str = "SELECT n.id FROM notes AS n WHERE n.title = :title ORDER BY n.id";

    fn params(&self) -> Vec<(&'static str, &dyn ToSql)> {
        vec![(":title", &self.title)]
    }
}

impl Returning for Titled {
    type Row = String;

    fn read(row: &Row<'_>) -> sqlive::rusqlite::Result<String> {
        row.get(0)
    }
}

impl Read for Titled {
    type Handle = Writer;
    const PLAN: &'static str = "";

    fn compiled() -> CompiledStatement<Writer> {
        static COMPILED: OnceLock<CompiledStatement<Writer>> = OnceLock::new();
        *COMPILED.get_or_init(|| compiled(Self::SQL))
    }
}

/// How many notes there are, as an app's build compiles the read.
struct Counted;

impl Statement for Counted {
    const SQL: &'static str = "SELECT count(*) FROM notes AS n";

    fn params(&self) -> Vec<(&'static str, &dyn ToSql)> {
        Vec::new()
    }
}

impl Returning for Counted {
    type Row = i64;

    fn read(row: &Row<'_>) -> sqlive::rusqlite::Result<i64> {
        row.get(0)
    }
}

impl Read for Counted {
    type Handle = Writer;
    const PLAN: &'static str = "";

    fn compiled() -> CompiledStatement<Writer> {
        static COMPILED: OnceLock<CompiledStatement<Writer>> = OnceLock::new();
        *COMPILED.get_or_init(|| compiled(Self::SQL))
    }
}

/// The read a request picks, its rows read as text whichever it is.
fn picked(titled: &Option<String>) -> Result<ReadAs<String>, DbError> {
    match titled {
        Some(title) => ReadAs::new(&Titled { title: title.clone() }, |id| id),
        None => ReadAs::new(&Counted, |count| count.to_string()),
    }
}

/// A live query whose request picks one of two compiled reads, each with its
/// own row type, reads each one's rows as one type, and runs again for a
/// write to what the read it picked reads.
#[tokio::test]
async fn a_request_picks_which_read_runs_and_its_rows_read_as_one_type() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes(id,title) VALUES('a','Plan')").await.unwrap();
    let mut query: crate::ReconfigurableLiveQuery<_, _, DbError> =
        db.subscribe_reconfigurable(None, picked);
    assert_eq!(query.next_inline().await.unwrap().result.unwrap(), ["1"]);
    query.requests().set(Some("Plan".into())).unwrap();
    assert_eq!(query.next_inline().await.unwrap().result.unwrap(), ["a"]);
    crate::write::tests::sql(&db, "INSERT INTO notes(id,title) VALUES('b','Plan')").await.unwrap();
    assert_eq!(query.next_inline().await.unwrap().result.unwrap(), ["a", "b"]);
    db.close().await.unwrap();
}

/// The reads a live query runs read once in a one-shot read, as they would
/// in a run: two reads on one snapshot return each one's rows.
#[tokio::test]
async fn a_one_shot_read_runs_the_reads_a_live_query_runs() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes(id,title) VALUES('a','Plan')").await.unwrap();
    let both = Both(Titled { title: "Plan".into() }, Counted);
    let read = db.read(move |sql| sql.reads(&both)).await.unwrap();
    assert_eq!(read, (vec!["a".to_owned()], vec![1]));
    let mut live = db.subscribe::<_, DbError>(Both(Titled { title: "Plan".into() }, Counted));
    assert_eq!(next_result(&mut live).await.unwrap(), read);
    db.close().await.unwrap();
}
