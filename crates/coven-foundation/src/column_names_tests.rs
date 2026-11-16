#![expect(
    unreachable_pub,
    reason = "the macro makes `name` public for the crates whose types use it; this test's own type is private"
)]

use super::UnknownName;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Season {
    Spring,
    Autumn,
}

crate::column_names!(Season, "season", { Spring => "spring", Autumn => "autumn" });

/// Each value binds as its name, reads back from the column, and parses
/// from the same text; text that names no value fails as `UnknownName`,
/// whether parsed or read from a column.
#[test]
fn a_value_binds_reads_and_parses_as_its_name() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    for season in [Season::Spring, Season::Autumn] {
        let text: String = db.query_row("SELECT ?1", [season], |r| r.get(0)).unwrap();
        assert_eq!(text, season.name());
        let read: Season = db.query_row("SELECT ?1", [season.name()], |r| r.get(0)).unwrap();
        assert_eq!(read, season);
        assert_eq!(season.name().parse::<Season>(), Ok(season));
    }
    let unknown = UnknownName { kind: "season", text: "Spring".into() };
    assert_eq!("Spring".parse::<Season>(), Err(unknown.clone()));
    let error = db.query_row("SELECT 'Spring'", [], |r| r.get::<_, Season>(0)).unwrap_err();
    assert!(
        matches!(&error, rusqlite::Error::FromSqlConversionFailure(_, _, cause)
            if cause.downcast_ref::<UnknownName>() == Some(&unknown)),
        "{error:?}"
    );
    assert!(matches!(
        db.query_row("SELECT 1", [], |r| r.get::<_, Season>(0)),
        Err(rusqlite::Error::InvalidColumnType(..))
    ));
}
