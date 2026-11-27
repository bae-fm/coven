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

/// What a policy that keeps the app out of the host's tables refuses with.
#[derive(Debug, thiserror::Error)]
#[error("the app may not read {0}")]
struct Refused(String);

/// The failure of a handle whose policy is `HostTables`. No handle fails in
/// these tests.
#[derive(Clone, Debug, thiserror::Error)]
#[error("the handle failed")]
struct Failed;

impl From<crate::EngineFailure<Failed, Refused>> for Failed {
    fn from(_failure: crate::EngineFailure<Failed, Refused>) -> Self {
        Self
    }
}

/// A policy that refuses the app's reads of a table whose name starts with
/// `host_`, and allows every other action.
struct HostTables;

impl crate::Policy for HostTables {
    type Refusal = Refused;
    type Failure = Failed;
    type Scope = ();

    fn judge(&self, action: &crate::PolicyAction<'_>) -> Result<(), Refused> {
        match action.action {
            rusqlite::hooks::AuthAction::Read { table_name, .. }
                if table_name.starts_with("host_")
                    && action.owner != crate::authorization::Owner::Host =>
            {
                Err(Refused(table_name.into()))
            }
            _ => Ok(()),
        }
    }

    fn check_sql(&self, _tokens: &[crate::sql::Token]) -> Result<(), Refused> {
        Ok(())
    }

    fn begin_app_call(&self) {}

    fn host_object(&self, name: &str) -> bool {
        name.starts_with("host_")
    }

    fn statement_key(&self) -> u64 {
        0
    }

    fn enter(&self, _scope: &()) {}

    fn leave(&self, _scope: &()) {}

    fn refresh(
        &self,
        _sql: &crate::HostSql<Self>,
        _extent: crate::authorization::Refresh,
    ) -> rusqlite::Result<()> {
        Ok(())
    }
}

/// The question a join puts to a write, whether the changed note's book is
/// one the statement reads, reads a table only the host may read, so the
/// write asks it as the host's own SQL.
#[test]
fn a_host_statements_join_questions_run_as_the_hosts_own_sql() {
    let directory = tempfile::tempdir().unwrap();
    let database = crate::HostSql::open(
        &directory.path().canonicalize().unwrap().join("host.sqlite"),
        false,
        HostTables,
        &crate::SqliteSettings::default(),
        &|_, _| Ok(()),
    )
    .unwrap();
    database
        .batch(
            "CREATE TABLE host_books(id TEXT PRIMARY KEY, name TEXT);
             CREATE TABLE host_notes(id INTEGER PRIMARY KEY, book TEXT, title TEXT);
             INSERT INTO host_books VALUES ('b1', 'x'), ('b2', 'y');
             INSERT INTO host_notes VALUES (1, 'b1', 'one')",
        )
        .unwrap();
    let new_note = |book: &str| {
        let columns: std::sync::Arc<[String]> =
            ["id", "book", "title"].map(String::from).to_vec().into();
        let values = [
            crate::SqlValue::Integer(2),
            crate::SqlValue::Text(book.into()),
            crate::SqlValue::Text("two".into()),
        ]
        .map(Some)
        .to_vec();
        crate::RowChange {
            table: "host_notes".into(),
            column: String::new(),
            rows: Some((None, Some(crate::RowImage::new(columns, values)))),
        }
    };
    let mut asked = Vec::new();
    let titles: Result<Vec<String>, crate::Error<Failed, Refused>> = database.live_read(
        &mut |reads, lookups| {
            for book in ["b2", "b1"] {
                let affected = crate::affected(
                    &reads,
                    &[new_note(book)],
                    &mut crate::Answers::default(),
                    lookups,
                )?;
                asked.push((book, affected));
            }
            Ok(())
        },
        |database| {
            database.host_live_read(
                "SELECT n.title FROM host_notes n JOIN host_books b ON b.id=n.book WHERE b.name=?1",
                ["x"],
                |row| row.get(0),
            )
        },
    );
    assert_eq!(titles.unwrap(), ["one"]);
    assert_eq!(asked, [("b2", false), ("b1", true)]);
}
