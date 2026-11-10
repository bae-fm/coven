use super::*;
use crate::internal_schema::FOLLOWED;
use crate::tests::TestStore;

/// Each object of the tables an app's triggers may follow, as
/// `(type, name, sql)`, and each table's row count, in table order.
type Followed = (Vec<(String, String, String)>, Vec<(String, i64)>);

const OBJECTS: &str = "SELECT type,name,sql FROM sqlite_schema
    WHERE type IN ('table','index') AND tbl_name=?1 ORDER BY type,name";

/// A connection outside any store, with the followed tables made on it.
fn outside_store() -> rusqlite::Connection {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    for table in followed_tables_sql() {
        connection.execute_batch(table).unwrap();
    }
    connection
}

/// The followed tables' SQL makes each table as a new store holds it: its
/// columns, constraints and indexes, and the one row of the sync status and
/// the retry effect.
#[tokio::test]
async fn the_followed_tables_are_made_as_a_store_holds_them() {
    let outside = outside_store();
    let mut objects = Vec::new();
    let mut rows = Vec::new();
    for followed in FOLLOWED {
        let mut statement = outside.prepare(OBJECTS).unwrap();
        let found = statement
            .query_map([followed.name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        objects.extend(found.map(Result::unwrap));
        let count = format!("SELECT count(*) FROM {}", followed.name);
        rows.push((followed.name.to_owned(), outside.query_row(&count, [], |r| r.get(0)).unwrap()));
    }
    let outside: Followed = (objects, rows);

    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let in_store: Followed = db.inspect_writer(|sql| {
        let mut objects = Vec::new();
        let mut rows = Vec::new();
        for followed in FOLLOWED {
            objects.extend(
                sql.query(OBJECTS, [followed.name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .unwrap(),
            );
            let count = format!("SELECT count(*) FROM {}", followed.name);
            rows.push((followed.name.to_owned(), sql.query_row(&count, [], |r| r.get(0)).unwrap()));
        }
        (objects, rows)
    });
    db.close().await.unwrap();
    assert_eq!(outside, in_store);
    assert_eq!(outside.0.iter().filter(|(kind, ..)| kind == "table").count(), FOLLOWED.len());
}

/// An app's trigger on each followed table applies outside a store, and
/// fires on the rows a test writes there.
#[test]
fn an_app_trigger_on_each_followed_table_fires_outside_a_store() {
    let outside = outside_store();
    outside.execute_batch("CREATE TABLE heard(what TEXT NOT NULL);").unwrap();
    for followed in FOLLOWED {
        outside
            .execute_batch(&format!(
                "CREATE TRIGGER heard_{name} AFTER UPDATE ON {name}
                 BEGIN INSERT INTO heard VALUES('{name}'); END;",
                name = followed.name,
            ))
            .unwrap();
    }
    outside
        .execute_batch(
            "UPDATE coven_sync_status SET starts=starts+1;
             UPDATE coven_retry_effect SET syncing=1;",
        )
        .unwrap();
    let heard: Vec<String> = outside
        .prepare("SELECT what FROM heard ORDER BY rowid")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(heard, ["coven_sync_status", "coven_retry_effect"]);
}
