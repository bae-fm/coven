use super::*;

#[test]
fn parent_root_search_stops_at_a_foreign_key_cycle() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE first (
                 id TEXT PRIMARY KEY,
                 second_id TEXT REFERENCES second(id)
             );
             CREATE TABLE second (
                 id TEXT PRIMARY KEY,
                 first_id TEXT REFERENCES first(id)
             );",
    )
    .unwrap();
    let tables = vec![
        SyncedTable::new("first", RowIdentity::SharedKey),
        SyncedTable::new("second", RowIdentity::SharedKey),
    ];
    let construction = GateModelConstruction::new(&conn, &tables);

    assert!(!construction
        .parent_reaches_root("first", &mut HashSet::new())
        .unwrap());
}
