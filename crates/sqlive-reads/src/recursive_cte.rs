//! A CTE whose SQL names itself, which sqlive reads whole, as it does a view
//! it can't read as cores of its own (SPEC.md section 8): for its values
//! alone, with every table it reads whole. A walk of its SQL (`sql_walk.rs`),
//! and of the CTEs and views that SQL reads, finds those tables and the names
//! SQLite can report a read through while it runs the CTE.

use std::collections::BTreeSet;

use crate::derived::{Ctes, Declared};
use crate::from_item::{Column, ColumnValue, Columns};
use crate::read_report::ReadPlace;
use crate::scope_plan::{Object, Planner, Source};
use crate::sql_walk::{Reads, Walk, Within};
use rusqlite::Connection;

/// What a recursive CTE reads.
struct Recursive {
    /// The tables, lowercase, its SQL reads, through the CTEs and views it
    /// reads too.
    tables: BTreeSet<String>,
    /// The names, lowercase, SQLite can report a read through while it runs
    /// the CTE: its own, and those of each CTE and view it runs; or the
    /// parser's message when it can't read a view the walk met.
    inside: Result<BTreeSet<String>, String>,
}

/// The FROM item that reads `declared`, named `qualifier`, and its columns,
/// when the CTE's SQL names itself; `inner` are the CTEs that SQL can name
/// (`Ctes::reading`).
pub(crate) fn read_recursive_item(
    connection: &Connection,
    planner: &mut Planner,
    declared: &Declared<'_>,
    inner: &Ctes<'_>,
    qualifier: String,
) -> rusqlite::Result<Option<(Source, Columns)>> {
    let Some(recursive) = read(planner, declared, inner)? else {
        return Ok(None);
    };
    planner.note_read_whole(recursive.inside.clone());
    let name = crate::sql::name(declared.cte.tbl_name.0).to_ascii_lowercase();
    let with = inner.around_every(&format!("SELECT * FROM {}", crate::sql::identifier(&name)));
    let (from, columns) = match with {
        Some(with) => {
            let statement = connection.prepare(&with)?;
            let columns = statement
                .column_names()
                .map(|column| Column {
                    name: Some(column.to_ascii_lowercase()),
                    value: ColumnValue::Unknown,
                })
                .collect();
            (Some(format!("({with}) AS {qualifier}")), Columns::Listed(columns))
        }
        // Its SQL, or that of a CTE it reads, may name a row around it.
        None => (None, Columns::Unknown { volatile: false }),
    };
    let source = Source {
        object: Object::Recursive { tables: recursive.tables, inside: recursive.inside.ok() },
        from,
        qualifier,
        term: None,
        carried: None,
        place: planner.within.clone().map(ReadPlace::Within),
        unread: false,
        columns: Default::default(),
        either: Vec::new(),
        memberships: Vec::new(),
    };
    Ok(Some((source, columns)))
}

/// What `declared` reads, when its SQL names itself, where that SQL can name
/// `inner`.
fn read(
    planner: &Planner,
    declared: &Declared<'_>,
    inner: &Ctes<'_>,
) -> rusqlite::Result<Option<Recursive>> {
    let at = inner
        .numbers()
        .position(|number| number == declared.number)
        .expect("a CTE's SQL can name the CTE");
    // First its own SQL alone, with the CTEs it can name only named: SQLite
    // runs a CTE that names itself only where its own SELECT names it.
    let mut walk = Walk::new(planner.schema, planner.views, ());
    let names = inner.withs().flatten().map(|declared| declared.name());
    let place = walk.name_ctes(names)[at];
    let reads = walk.select(declared.cte.select, false)?;
    if !walk.scope.run(&reads).contains(&Within::Cte(place)) {
        return Ok(None);
    }
    // Then every CTE it can name, each WITH's SQL where it can name what
    // SQLite resolves it to.
    let mut walk = Walk::new(planner.schema, planner.views, ());
    let mut places = Vec::new();
    for with in inner.withs() {
        places.extend(walk.declare(with.iter().map(|declared| declared.cte))?);
    }
    let reads = Reads { ctes: BTreeSet::from([places[at]]), ..Reads::default() };
    let tables = walk.scope.tables(&reads).iter().map(|table| table.to_ascii_lowercase()).collect();
    let inside = match walk.scope.unread() {
        Some(why) => Err(why.to_owned()),
        None => Ok(walk
            .scope
            .run(&reads)
            .iter()
            .map(|within| walk.scope.name_of(within).to_owned())
            .collect()),
    };
    Ok(Some(Recursive { tables, inside }))
}
