//! Test support for the published merge state's tables: filling them for the
//! plan audit, and the full scans their statements' plans may contain.

/// Fill the published merge state's tables, thousands of rows each but one marker,
/// from the rows of the plan audit's `fixture` table, inside its transaction
/// (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_published VALUES(1);
         INSERT INTO _coven_published_rows SELECT tab,key,audience,x'00',i%2 FROM fixture;
         INSERT INTO _coven_published_losses SELECT i,tab,key,audience,x'00' FROM fixture;
         INSERT INTO _coven_published_creations SELECT tab,key,audience,CAST(device || n AS BLOB),x'00' FROM fixture;
         INSERT INTO _coven_published_file_refs SELECT device,substr(key,17,16) FROM fixture;",
    )
    .unwrap();
}

/// The full scans the published merge state's statements'
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
("published.rs", "SELECT EXISTS(SELECT 1 FROM _coven_published)", "_coven_published") =>
    Some("the counted projection marker has at most one row"),
("published.rs", "SELECT table_name,key,audience FROM _coven_rows UNION SELECT table_name,key,audience FROM _coven_lost", "_coven_rows" | "_coven_lost") =>
    Some("preserving the counted projection visits all current row identities"),
("published.rs", "DELETE FROM {table}", table) if sql == format!("DELETE FROM {table}") =>
    Some("releasing the counted projection clears all of its retained records"),
("published.rs", "SELECT table_name,key,audience FROM _coven_published_rows UNION SELECT table_name,key,audience FROM _coven_published_losses", "_coven_published_rows" | "_coven_published_losses") =>
    Some("restoration visits every saved row identity"),
("published.rs", "SELECT record FROM _coven_published_rows ORDER BY audience,table_name,key", "_coven_published_rows") =>
    Some("restoration reads every preserved row"),
("published.rs", "SELECT record FROM _coven_published_losses ORDER BY id", "_coven_published_losses") =>
    Some("restoration reads every preserved loss"),
("published.rs", "INSERT INTO _coven_creations SELECT * FROM _coven_published_creations", "_coven_published_creations") =>
    Some("restoration installs every preserved creation"),
("published_migration.rs", "CREATE TEMP TABLE _coven_carried_rows AS SELECT * FROM _coven_published_rows; DELETE FROM _coven_published_rows", "_coven_published_rows") =>
    Some("a migration carries every preserved row through schema changes"),
_ => None,
    }
}
