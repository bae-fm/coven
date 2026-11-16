use super::*;
use crate::tests::{TestDatabase, TestError};
use std::borrow::Cow;

/// The path SQLite opens for `path`.
fn plain_path(path: &Path) -> Result<Cow<'_, Path>, TestError> {
    sqlite_path(path)
}

/// SQLite names a database by the path its host gives, which on Windows
/// doesn't start `\\`, so SQLite locks it as a local file.
#[test]
fn sqlite_opens_the_database_by_its_plain_path() {
    let db = TestDatabase::open("");
    let file: String = db.value("SELECT file FROM pragma_database_list WHERE name='main'");
    assert_eq!(Path::new(&file), plain_path(db.path()).unwrap());
    assert!(!file.starts_with(r"\\"), "{file}");
    db.close();
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
        assert_eq!(plain_path(Path::new(verbatim)).unwrap(), Path::new(plain));
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
        assert!(
            matches!(plain_path(Path::new(verbatim)), Err(crate::Error::Path { .. })),
            "{verbatim}"
        );
    }
}
