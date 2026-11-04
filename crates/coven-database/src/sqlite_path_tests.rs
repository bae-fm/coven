use super::*;
use crate::tests::TestStore;

/// SQLite names the store's database by the path coven gives it, which on
/// Windows doesn't start `\\`, so SQLite locks it as a local file.
#[tokio::test]
async fn sqlite_opens_the_database_by_its_plain_path() {
    let store = TestStore::new();
    let db = store.builder(Vec::new(), Vec::new()).open().await.unwrap();
    let mut file = String::new();
    db.inspect_writer(|writer| {
        file = writer
            .query_row("SELECT file FROM pragma_database_list WHERE name='main'", [], |r| r.get(0))
            .unwrap();
    });
    let database = store.database_path();
    assert_eq!(Path::new(&file), sqlite_path(&database).unwrap());
    assert!(!file.starts_with(r"\\"), "{file}");
    db.close().await.unwrap();
}

#[cfg(windows)]
#[test]
fn a_verbatim_path_goes_to_sqlite_in_its_plain_form() {
    for (verbatim, plain) in [
        (r"\\?\D:\a\_temp\stores\store.db", r"D:\a\_temp\stores\store.db"),
        (r"\\?\UNC\server\share\stores\store.db", r"\\server\share\stores\store.db"),
        (r"D:\stores\store.db", r"D:\stores\store.db"),
        // Windows opens no device for COM0 or LPT0.
        (r"\\?\D:\COM0\store.db", r"D:\COM0\store.db"),
        (r"\\?\D:\lpt0.music\store.db", r"D:\lpt0.music\store.db"),
    ] {
        assert_eq!(sqlite_path(Path::new(verbatim)).unwrap(), Path::new(plain));
    }
}

#[cfg(windows)]
#[test]
fn a_name_windows_reads_otherwise_without_the_prefix_is_refused() {
    for verbatim in [
        r"\\?\D:\stores.\store.db",
        r"\\?\D:\stores \store.db",
        r"\\?\D:\NUL\store.db",
        r"\\?\D:\com1.music\store.db",
        r"\\?\D:\stores\..\store.db",
    ] {
        assert!(matches!(sqlite_path(Path::new(verbatim)), Err(DbError::Disk(_))), "{verbatim}");
    }
}
