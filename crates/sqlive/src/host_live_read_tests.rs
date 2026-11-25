use crate::tests::{next, QueryError, TestDatabase, TestRead};

const TITLE: &str = "SELECT title FROM notes WHERE id = ?1 ORDER BY id";

fn notes() -> TestDatabase {
    TestDatabase::open(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
         INSERT INTO notes VALUES ('1', 'one'), ('2', 'two')",
    )
}

fn titles(reader: TestRead<'_>, id: &str) -> Result<Vec<String>, QueryError> {
    Ok(reader.0.host_live_read(TITLE, [id], |row| row.get(0))?)
}

/// A host statement in a live query's read runs again for the writes that
/// change the rows its plan names, and for no others.
#[tokio::test]
async fn a_host_statement_narrows_as_an_app_statement_does() {
    let db = notes();
    let mut live = db.subscribe(|reader| titles(reader, "1"));
    assert_eq!(next(&mut live).await, ["one"]);
    db.commit(
        "UPDATE notes SET title = 'other' WHERE id = '2'; INSERT INTO notes VALUES ('3', 'three')",
    );
    assert!(!live.is_marked_for_rerun());
    db.commit("UPDATE notes SET title = 'first' WHERE id = '1'");
    assert_eq!(next(&mut live).await, ["first"]);
    db.close();
}

/// Outside a live query's read a host statement reads rows and records
/// nothing.
#[test]
fn a_host_statement_in_a_one_shot_read_returns_its_rows() {
    let db = notes();
    assert_eq!(db.read_app(|reader| titles(reader, "2")).unwrap(), ["two"]);
    db.close();
}

/// A host statement's plan is built once for the database, for every reader.
#[test]
fn readers_share_a_host_statements_plan() {
    let db = notes();
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    let mut built = Vec::new();
    for _ in 0..2 {
        db.with_database(|database| {
            let (first, second) = (database.reader(), database.reader());
            for reader in [&first, &second] {
                let rows = reader.host(|reader| {
                    reader.live_read(&mut |_reads, _lookups| Ok(()), |reader| {
                        titles(TestRead(reader), "1")
                    })
                });
                assert_eq!(rows.unwrap(), ["one"]);
            }
        });
        built.push(counter.take().scope_plans_built);
    }
    assert_eq!(built, [1, 0]);
    db.close();
}
