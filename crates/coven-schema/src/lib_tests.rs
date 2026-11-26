use super::*;

/// The text for an app's build script makes every object app SQL may read,
/// each as a `CREATE` statement.
#[test]
fn the_readable_text_creates_each_object_app_sql_reads() {
    let sql = readable_objects_sql();
    for name in READABLE {
        let table = format!("CREATE TABLE {name} ");
        let view = format!("CREATE VIEW {name}(");
        let view_without_columns = format!("CREATE VIEW {name} ");
        assert!(
            sql.contains(&table) || sql.contains(&view) || sql.contains(&view_without_columns),
            "{name}"
        );
    }
}

/// The text makes the tables the views read, and none of the store's others.
#[test]
fn the_readable_text_makes_the_tables_outside_a_store_holds() {
    let sql = readable_objects_sql();
    for name in OUTSIDE_STORE_TABLES {
        assert!(sql.contains(&format!("CREATE TABLE {name} ")), "{name}");
    }
    assert!(!sql.contains("_coven_rows"));
    assert!(initial_schema().contains("_coven_rows"));
}
