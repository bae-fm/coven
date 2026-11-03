//! Test support for the membership and audience tables: filling them for
//! the plan audit, damaging them as broken storage would, and the full scans
//! their statements' plans may contain.

/// Fill the membership and audience tables, thousands of rows each,
/// from the rows of the plan audit's `fixture` table, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_members SELECT member,key,key,x'00','member',i%2 FROM fixture;
         INSERT INTO _coven_bound_keys SELECT key,key FROM fixture;
         INSERT INTO _coven_devices SELECT n,member,'device ' || i,CASE i%3 WHEN 0 THEN 'active' WHEN 1 THEN 'removed' ELSE 'replaced' END FROM fixture;
         INSERT INTO _coven_circles SELECT 'circle_' || i,'circle ' || i FROM fixture;
         INSERT INTO _coven_circle_members SELECT 'circle_' || i,member FROM fixture;
         INSERT INTO _coven_circle_deletions SELECT 'circle_' || i,n FROM fixture;",
    )
    .unwrap();
}

/// Give every member that isn't an admin a role no write stores, as damaged
/// storage would, for tests of what reading members does.
pub(crate) fn damage_roles(db: &crate::sqlite::DatabaseConnection) {
    db.batch(
        "PRAGMA ignore_check_constraints=ON;
         UPDATE _coven_members SET role='owner' WHERE role='member';
         PRAGMA ignore_check_constraints=OFF;",
    )
    .unwrap()
}

/// The full scans the membership and audience tables' statements'
/// plans may contain, each with its reason, as the plan audit asks
/// (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
("store_state_tables.rs", "SELECT member,signing,sealing,role,removed,access FROM _coven_members", "_coven_members") =>
    Some("store_state() returns the complete member map"),
("store_state_tables.rs", "SELECT signing,sealing FROM _coven_bound_keys", "_coven_bound_keys") =>
    Some("store_state() returns every registered key pair"),
("store_state_tables.rs", "SELECT device,member,name,state FROM _coven_devices", "_coven_devices") =>
    Some("store_state() returns the complete device map"),
("store_state_tables.rs", "SELECT circle,name FROM _coven_circles", "_coven_circles") =>
    Some("store_state() returns the complete circle map"),
("store_state_tables.rs", "SELECT circle,member FROM _coven_circle_members", "_coven_circle_members") =>
    Some("store_state() returns every circle membership"),
("store_state_tables.rs", "SELECT circle,slot FROM _coven_circle_deletions", "_coven_circle_deletions") =>
    Some("store_state() returns every circle deletion and its cause"),
("store_state_tables.rs", "DELETE FROM {table}", table)
    if sql == format!("DELETE FROM {table}") =>
    Some("applying a slot replaces the entire derived table; foreign-key lookups must be indexed"),
_ => None,
    }
}
