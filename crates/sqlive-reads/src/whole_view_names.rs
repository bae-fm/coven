//! The names SQLite can report a read through while it runs a view that
//! sqlive reads whole (§4). SQLite's authorizer names the innermost view or
//! CTE each read is made through, so a read inside such a view comes with
//! the view's own name, the name of a CTE its SQL declares, or a name from
//! a view it reads. A statement's own CTE can share such a name, so a read
//! reported through it may belong to the view rather than to the CTE.

use std::collections::BTreeSet;

use crate::sql_walk::Walk;
use rusqlite::Connection;

/// The names, lowercase, SQLite can report a read through while it runs
/// the main schema's view `stored`: its own, each CTE its SQL declares, and
/// those of each view its SQL reads (`sql_walk.rs`); or the parser's
/// message when it can't read one of those views, whose SQL could declare
/// any name.
pub(crate) fn whole_view_names(
    connection: &Connection,
    stored: &str,
) -> rusqlite::Result<Result<BTreeSet<String>, String>> {
    let mut walk = Walk::new(connection, ());
    walk.view(stored)?;
    Ok(match walk.scope.unread() {
        Some(why) => Err(why.to_owned()),
        None => Ok(walk.scope.names()),
    })
}
