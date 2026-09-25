//! Incoming deletes of an ancestor that a remaining child still references.

use std::collections::HashSet;

use rusqlite::Connection;

use super::ffi::{collect_deletes, for_each_change, Changegroup};
use super::model::TableGate;
use super::{GateError, Gates};
use crate::quote_ident;

/// Drop each DELETE of a `gated_by_descendants` ancestor row that one of its
/// inferred children still references once `changeset` applies.
///
/// An ancestor leaves a device's shared set when its last kept child goes, and
/// that device's write retracts it with a DELETE. Another device can, while
/// apart, give the same ancestor a new child; its write carries the ancestor
/// again, but when that write is applied first the ancestor is already there.
/// The ancestor is shared exactly while some child keeps it, so the concurrent
/// child keeps it alive: the retract does not remove a row a remaining child
/// references. A DELETE whose children this same changeset also deletes still
/// applies. An ancestor kept this way keeps its own ancestors in turn. The
/// rule reads only the projection the changeset applies to, so
/// every device applying the same history keeps the same ancestors.
pub(crate) fn retain_referenced_ancestors(
    conn: &Connection,
    gates: &Gates,
    changeset: &[u8],
) -> Result<Vec<u8>, GateError> {
    // SAFETY: every iterator handed to `add_change` is the one `for_each_change`
    // is positioned on, and `conn` outlives the changegroup.
    unsafe {
        let deleted = collect_deletes(changeset)?;
        // A retained ancestor still references its own ancestors, so retaining
        // one can retain the next level up: repeat until nothing changes.
        let mut referenced = HashSet::new();
        loop {
            let mut grew = false;
            for (key, row) in &deleted {
                if referenced.contains(key) {
                    continue;
                }
                let Some(TableGate::Parent { children }) = gates.tables.get(&key.0) else {
                    continue;
                };
                for (child, fk_col, parent_col) in children {
                    let Some(Some(parent_value)) = row.old.get(parent_col.index) else {
                        continue;
                    };
                    if child_references(
                        conn,
                        &deleted,
                        &referenced,
                        child,
                        &fk_col.name,
                        parent_value,
                    )? {
                        referenced.insert(key.clone());
                        grew = true;
                        break;
                    }
                }
            }
            if !grew {
                break;
            }
        }
        if referenced.is_empty() {
            return Ok(changeset.to_vec());
        }
        let group = Changegroup::new()?;
        group.set_schema(conn.handle())?;
        for_each_change(changeset, |iter, row| {
            let retained = row.op == rusqlite::ffi::SQLITE_DELETE
                && row
                    .pk()
                    .is_some_and(|pk| referenced.contains(&(row.table.clone(), pk.to_string())));
            if retained {
                Ok(())
            } else {
                group.add_change(iter)
            }
        })?;
        group.output()
    }
}

/// Whether a row of `child` that stays — one `deleted` does not remove, or
/// one `retained` keeps — references `parent_value` through `fk_col`.
fn child_references(
    conn: &Connection,
    deleted: &std::collections::HashMap<(String, String), super::ffi::ChangeRow>,
    retained: &HashSet<(String, String)>,
    child: &str,
    fk_col: &str,
    parent_value: &str,
) -> Result<bool, GateError> {
    let sql = format!(
        "SELECT {id} FROM {child} WHERE {fk} = ?1",
        id = quote_ident("id"),
        child = quote_ident(child),
        fk = quote_ident(fk_col),
    );
    let mut statement = conn
        .prepare(&sql)
        .map_err(|error| GateError::Sql(format!("read children of {child}"), error))?;
    let ids = statement
        .query_map([parent_value], |row| row.get::<_, String>(0))
        .map_err(|error| GateError::Sql(format!("read children of {child}"), error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GateError::Sql(format!("read children of {child}"), error))?;
    Ok(ids.into_iter().any(|id| {
        let key = (child.to_string(), id);
        !deleted.contains_key(&key) || retained.contains(&key)
    }))
}
