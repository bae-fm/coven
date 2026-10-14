use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_merge::ForeignKey;

type Registered = (Vec<(String, String)>, Vec<(String, ForeignKey)>);

fn registered(db: &Database) -> Registered {
    db.inspect_writer(|db| {
        let mut columns = db
            .query(
                "SELECT table_name,column_name FROM _coven_columns",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let mut keys = db
            .query(
                "SELECT table_name,identity FROM _coven_foreign_keys",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        coven_format::merge_fields::decode_foreign_key(&r.get::<_, Vec<u8>>(1)?)
                            .unwrap(),
                    ))
                },
            )
            .unwrap();
        columns.sort();
        keys.sort();
        (columns, keys)
    })
}

fn expected(columns: &[(&str, &[&str])], keys: &[(&str, &str)]) -> Registered {
    let mut columns: Vec<_> = columns
        .iter()
        .flat_map(|(table, names)| {
            names
                .iter()
                .map(|name| ((*table).to_owned(), (*name).to_owned()))
        })
        .collect();
    let mut keys: Vec<_> = keys
        .iter()
        .map(|(table, column)| {
            (
                (*table).to_owned(),
                ForeignKey::new([*column], "notes", ["id"]),
            )
        })
        .collect();
    columns.sort();
    keys.sort();
    (columns, keys)
}

/// §8: `_coven_columns` has one row per synced column and `_coven_foreign_keys`
/// one per foreign key of a synced table, before any write names them, through
/// additions, a dropped column and a renamed one. Local tables have none, and
/// writes only look the rows up.
#[tokio::test]
async fn every_synced_column_and_foreign_key_is_registered_before_any_write_names_it() {
    let steps = [
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE,other TEXT REFERENCES notes(id) ON DELETE SET NULL); CREATE TABLE pins(note_id TEXT REFERENCES notes(id) ON DELETE CASCADE)",
        "CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE); ALTER TABLE links ADD COLUMN extra TEXT",
        "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE,extra TEXT); INSERT INTO rebuilt SELECT id,note_id,extra FROM links; DROP TABLE links; ALTER TABLE rebuilt RENAME TO links",
        "ALTER TABLE links RENAME COLUMN note_id TO note",
    ];
    let migrations = |through: usize| {
        steps[..through]
            .iter()
            .enumerate()
            .map(|(index, sql)| Migration::sql(index as u32 + 1, "step", sql))
            .collect::<Vec<_>>()
    };
    let tables = |tags: bool| {
        let mut tables = vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("links", RowIdentity::SharedKey),
        ];
        if tags {
            tables.push(SyncedTable::new("tags", RowIdentity::SharedKey));
        }
        tables
    };
    let stages = [
        expected(
            &[("notes", &["id"]), ("links", &["id", "note_id", "other"])],
            &[("links", "note_id"), ("links", "other")],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note_id", "other", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[
                ("links", "note_id"),
                ("links", "other"),
                ("tags", "note_id"),
            ],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note_id", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[("links", "note_id"), ("tags", "note_id")],
        ),
        expected(
            &[
                ("notes", &["id"]),
                ("links", &["id", "note", "extra"]),
                ("tags", &["id", "note_id"]),
            ],
            &[("links", "note"), ("tags", "note_id")],
        ),
    ];
    let store = TestStore::new();
    for (index, stage) in stages.iter().enumerate() {
        let db = store
            .builder(tables(index > 0), migrations(index + 1))
            .open()
            .await
            .unwrap();
        assert_eq!(&registered(&db), stage, "after step {}", index + 1);
        db.close().await.unwrap();
    }
    let db = store
        .builder(tables(true), migrations(steps.len()))
        .open()
        .await
        .unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('n'); INSERT INTO links VALUES('l','n','x'); INSERT INTO tags VALUES('t','n'); INSERT INTO pins VALUES('n')",
    )
    .await
    .unwrap();
    assert_eq!(&registered(&db), stages.last().unwrap());
    db.close().await.unwrap();
}
