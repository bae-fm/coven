use super::*;
use crate::tests::TestStore;
use crate::write::tests::{notes, NOTES};

/// One read, written once for either context.
fn pantry_title(sql: &impl SqlRead) -> rusqlite::Result<String> {
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
    let read = database.read(|context| Ok(pantry_title(&context)?)).await.unwrap();
    assert_eq!(read, "Pantry");
    database.close().await.unwrap();
}
