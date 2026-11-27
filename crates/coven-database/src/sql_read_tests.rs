use super::*;
use crate::tests::TestStore;
use crate::write::tests::{notes, NOTES};
use crate::DbError;

/// One read, written once for either context.
fn pantry_title(sql: &impl SqlRead) -> sqlive::rusqlite::Result<String> {
    sql.query_row("SELECT title FROM notes WHERE id='pantry'", [], |r| r.get(0))
}

/// A read written for either context runs inside a write, where it sees what
/// the write has changed, and in a read, where it sees what committed.
#[tokio::test]
async fn one_read_runs_inside_a_write_and_in_a_read() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    let seen = database
        .write(|context| {
            context.execute("INSERT INTO notes(id,title) VALUES('pantry','Pantry')", [])?;
            Ok(pantry_title(&context)?)
        })
        .await
        .unwrap();
    assert_eq!(seen, "Pantry");
    let read = database.read(|context| Ok::<_, DbError>(pantry_title(&context)?)).await.unwrap();
    assert_eq!(read, "Pantry");
    database.close().await.unwrap();
}

const PANTRY: &str = "SELECT title FROM notes WHERE id = :id";

/// `PANTRY` with the scope plan built against `schema`, as an app's build
/// compiles it.
fn compiled(schema: &str) -> sqlive::CompiledStatement {
    let probe = sqlive::SchemaProbe::open(&[schema.to_owned()], &[], &[], &[]).unwrap();
    let plan = probe.compiled_plan(PANTRY).unwrap().unwrap().encode();
    sqlive::CompiledStatement { sql: PANTRY, plan: Box::leak(plan.into_boxed_str()) }
}

fn compiled_title(
    sql: &impl SqlRead,
    statement: sqlive::CompiledStatement,
) -> sqlive::rusqlite::Result<Vec<String>> {
    sql.query_compiled(statement, &[(":id", &"pantry")], |r| r.get(0))
}

/// A read compiled against the app's schema runs inside a write and in a
/// read, on the store's database, whose schema also holds coven's objects.
/// One compiled against another definition of the table it reads fails,
/// naming the table.
#[tokio::test]
async fn a_compiled_read_runs_in_either_context_and_fails_on_another_schema() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    let statement = compiled(NOTES);
    let seen = database
        .write(move |context| {
            context.execute("INSERT INTO notes(id,title) VALUES('pantry','Pantry')", [])?;
            Ok(compiled_title(&context, statement)?)
        })
        .await
        .unwrap();
    assert_eq!(seen, ["Pantry"]);
    let read = database
        .read(move |context| Ok::<_, DbError>(compiled_title(&context, statement)?))
        .await
        .unwrap();
    assert_eq!(read, ["Pantry"]);
    let other = compiled(&NOTES.replace("body TEXT", "summary TEXT"));
    let failed = database
        .read(move |context| Ok::<_, DbError>(compiled_title(&context, other)?))
        .await
        .unwrap_err();
    match failed {
        DbError::CompiledSchemaDiffers(differs) => assert_eq!(differs.differ, ["table notes"]),
        other => panic!("expected the compiled schema to differ, got {other:?}"),
    }
    database.close().await.unwrap();
}
