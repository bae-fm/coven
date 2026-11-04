use super::*;

const SCHEMA: &str = "CREATE TABLE notes (id TEXT);
CREATE TABLE note_listing (id TEXT);
CREATE TABLE note_search (id TEXT);
CREATE VIEW listing_refresh AS SELECT id FROM note_listing;
CREATE TRIGGER listing_refreshed INSTEAD OF INSERT ON listing_refresh BEGIN
    DELETE FROM note_listing WHERE id = new.id;
    INSERT INTO note_listing VALUES (new.id);
    INSERT INTO search_refresh VALUES (new.id);
END;
CREATE VIEW search_refresh AS SELECT id FROM note_search;
CREATE TRIGGER search_refreshed INSTEAD OF INSERT ON search_refresh BEGIN
    INSERT INTO note_search VALUES (new.id);
END;
CREATE TRIGGER search_cleared INSTEAD OF DELETE ON search_refresh BEGIN
    DELETE FROM note_search WHERE id = old.id;
END;
CREATE TEMP VIEW staged AS SELECT 1;";

fn names(tables: BTreeSet<String>) -> Vec<String> {
    tables.into_iter().collect()
}

/// A write to a view is its `INSTEAD OF` triggers' writes for that
/// operation, through views in turn; a table is itself.
#[test]
fn a_write_to_a_view_is_its_instead_of_triggers_writes() {
    let views = SchemaViews::collect([SCHEMA]);
    assert_eq!(
        names(views.tables("listing_refresh", Operation::Insert)),
        ["note_listing", "note_search"]
    );
    assert_eq!(names(views.tables("search_refresh", Operation::Delete)), ["note_search"]);
    assert!(views.tables("listing_refresh", Operation::Update).is_empty());
    assert_eq!(names(views.tables("notes", Operation::Update)), ["notes"]);
    assert_eq!(names(views.tables_behind("search_refresh")), ["note_search"]);
    assert!(!views.is_view("staged"));
}

/// Views whose triggers write each other end: each view and operation is
/// expanded once.
#[test]
fn views_writing_each_other_expand_once() {
    let views = SchemaViews::collect([
        "CREATE VIEW a AS SELECT 1; CREATE VIEW b AS SELECT 1;
         CREATE TRIGGER a_in INSTEAD OF INSERT ON a BEGIN INSERT INTO b VALUES (1); INSERT INTO notes VALUES (1); END;",
        "CREATE TRIGGER b_in INSTEAD OF INSERT ON b BEGIN INSERT INTO a VALUES (1); END;",
    ]);
    assert_eq!(names(views.tables("b", Operation::Insert)), ["notes"]);
}
