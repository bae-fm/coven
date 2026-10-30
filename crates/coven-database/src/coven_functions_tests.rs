use crate::tests::TestStore;
use crate::{CovenError, DbError, Migration, RowIdentity, SyncedTable};

/// A store whose planner reads an analyzed index of coven keys.
async fn analyzed_keys(store: &TestStore) -> crate::Database {
    store
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![
                Migration::addition(
                    1,
                    "notes",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
                ),
                Migration::addition(
                    2,
                    "keys",
                    "CREATE TABLE keys(key BLOB NOT NULL); CREATE INDEX keys_key ON keys(key); INSERT INTO keys VALUES (coven_key('a')),(coven_key('b')),(coven_key(7,'a')); ANALYZE keys",
                ),
            ],
        )
        .open()
        .await
        .unwrap()
}

/// SQLite evaluates a deterministic function of no arguments while it plans a
/// statement against an analyzed index, with a null argument array that
/// rusqlite can't receive. `coven_key()` with no values reaches a varying
/// overload instead, which SQLite calls only as the statement runs, and which
/// refuses it rather than the process ending.
#[tokio::test]
async fn coven_key_without_arguments_is_refused_where_the_planner_reads_statistics() {
    let store = TestStore::new();
    let db = analyzed_keys(&store).await;
    let error = db
        .read(|sql| {
            Ok(
                sql.query("SELECT key FROM keys WHERE key = coven_key()", [], |r| {
                    r.get::<_, Vec<u8>>(0)
                })?,
            )
        })
        .await
        .unwrap_err();
    match error {
        CovenError::Database(DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(msg)))) => {
            assert_eq!(msg, "coven_key takes a row's key: one value or more")
        }
        error => panic!("expected coven_key's refusal, received {error:?}"),
    }
    // With its arguments, the same comparison reads the index.
    let found = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT count(*) FROM keys WHERE key = coven_key(7,'a')",
                [],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(found, [1]);
    db.close().await.unwrap();
}

/// A key as wide as an SQL call can pass encodes, and one wider fails as
/// SQLite's own limit does.
#[tokio::test]
async fn coven_key_takes_every_width_an_sql_call_can_pass() {
    let store = TestStore::new();
    let db = analyzed_keys(&store).await;
    let widest = db
        .inspect_writer(|sql| sql.function_argument_limit())
        .unwrap();
    for width in [1, 2, widest] {
        let arguments = vec!["1"; width].join(",");
        let statement = format!("SELECT coven_key({arguments})");
        let key: Vec<u8> = db
            .read(move |sql| Ok(sql.query_row(&statement, [], |r| r.get(0))?))
            .await
            .unwrap();
        let expected =
            coven_format::key::encode_key(&vec![coven_format::value::Value::Integer(1); width])
                .unwrap();
        assert_eq!(key, expected, "{width}");
    }
    let arguments = vec!["1"; widest + 1].join(",");
    let statement = format!("SELECT coven_key({arguments})");
    assert!(db
        .read(move |sql| Ok(sql.query_row(&statement, [], |r| r.get::<_, Vec<u8>>(0))?))
        .await
        .is_err());
    db.close().await.unwrap();
}

/// `coven_path_text` shows a recorded path as `path_text` shows the path,
/// so a name an app made at import matches what its SQL shows. NULL stays
/// NULL, and anything but a path's bytes is refused.
#[tokio::test]
async fn coven_path_text_shows_what_path_text_shows() {
    use coven_foundation::files::{path_bytes, path_text};
    use std::path::PathBuf;
    let store = TestStore::new();
    let db = analyzed_keys(&store).await;
    let mut paths: Vec<PathBuf> = [
        "Abbey Road/01 Come Together.flac",
        "Björk/Homogenic/Jóga.flac",
        r"Caf\xE9.flac",
        "",
    ]
    .into_iter()
    .map(PathBuf::from)
    .collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        for bytes in [&b"Caf\xE9.flac"[..], b"\xFF\xFE/Bj\xC3", b"\xED\xA0\x80"] {
            paths.push(std::ffi::OsStr::from_bytes(bytes).into());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        for units in [&[0x43, 0x61, 0x66, 0xD800][..], &[0xDC00, 0xD800]] {
            paths.push(std::ffi::OsString::from_wide(units).into());
        }
    }
    for path in paths {
        let bytes = path_bytes(&path);
        let shown: String = db
            .read(
                move |sql| Ok(sql.query_row("SELECT coven_path_text(?1)", [bytes], |r| r.get(0))?),
            )
            .await
            .unwrap();
        assert_eq!(shown, path_text(&path), "{path:?}");
    }
    let null: Option<String> = db
        .read(|sql| Ok(sql.query_row("SELECT coven_path_text(NULL)", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(null, None);
    assert!(db
        .read(
            |sql| Ok(sql.query_row("SELECT coven_path_text('text')", [], |r| r
                .get::<_, String>(0))?)
        )
        .await
        .is_err());
    db.close().await.unwrap();
}
