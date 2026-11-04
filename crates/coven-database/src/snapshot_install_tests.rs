/// The scans this module's statements may make, with their reasons, for the
/// plan audit (`internal_schema_scan_reasons.rs`): `template` is the statement as
/// written, `sql` as run.
pub(crate) fn scan_reason(
    file: &str,
    template: &str,
    _sql: &str,
    relation: &str,
) -> Option<&'static str> {
    match (file, template, relation) {
("snapshot_install.rs", sql, "g")
    if sql.starts_with("INSERT INTO _coven_rows(") =>
    Some("installing writes each earlier generation of the snapshot's rows; its write lookup is indexed"),
("snapshot_install.rs", sql, "i")
    if sql.starts_with("INSERT INTO _coven_rows(")
        || sql.starts_with("INSERT INTO _coven_cells(")
        || sql.starts_with("INSERT INTO _coven_references(") =>
    Some("installing writes the merge records of every row of the snapshot, one statement per table, column and reference; each lookup is indexed"),
("snapshot_install.rs", sql, "i")
    if sql.starts_with("SELECT EXISTS(SELECT 1 FROM {installed} AS i") =>
    Some("installing checks every row of the snapshot once; its loss lookup is indexed"),
("snapshot_install.rs", sql, relation)
    if (relation == "s" || relation.starts_with("temp._coven_install_"))
        && (sql.contains("temp._coven_install_parts") || sql.contains("temp._coven_install_authored")) =>
    Some("installing copies and checks every source record of the snapshot once; each lookup of the device's own is indexed"),
        _ => None,
    }
}
