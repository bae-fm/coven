//! The columns that SQL which reads a table function names without their
//! tables. sqlive doesn't know a table function's columns, so beside one it
//! can't tell which table such a name belongs to (SPEC.md section 5), and
//! sqlive-compose refuses the SQL.

use std::collections::BTreeSet;

use sqlite3_parser::ast::{
    As, Expr, FromClause, Name, OneSelect, ResultColumn, Select, SelectTable,
};
use sqlite3_parser::Bump;

use crate::compiled_plan::Unanalyzable;
use crate::schema_objects::{MainSchema, SchemaLookups};
use crate::select_syntax::one_select;
use crate::sql_walk::{Reads, Scope, Visit, Walk};
use crate::view_trees::ViewTrees;

/// The first column that `sql`, a SELECT, names without its table, when it
/// reads a table function anywhere. A name of a result column that is a whole
/// ORDER BY term, which no table qualifies, is none, and neither are `true`
/// and `false`. SQL the parser can't read as one SELECT, which sqlive can't
/// analyze, gives why.
pub fn bare_column_beside_table_function(
    sql: &str,
) -> rusqlite::Result<Result<Option<String>, Unanalyzable>> {
    let bump = Bump::new();
    let select = match one_select(&bump, sql) {
        Ok(select) => select,
        Err(why) => return Ok(Err(Unanalyzable { why: why.to_string() })),
    };
    // The walk looks each FROM item up in a schema with no object, so it
    // reads no view's SQL, which the call didn't write.
    let schema = MainSchema::default();
    let schema = SchemaLookups::new(&schema);
    let views = ViewTrees::new(&bump);
    let mut walk = Walk::new(&schema, &views, BareNames::default());
    walk.select(select, false)?;
    let BareNames { table_function, names, result_names } = walk.visit;
    let bare = names.into_iter().find(|(address, _)| !result_names.contains(address));
    Ok(Ok(bare.filter(|_| table_function).map(|(_, name)| name)))
}

/// What the walk of a statement finds.
#[derive(Default)]
struct BareNames {
    /// Whether a FROM reads a table function.
    table_function: bool,
    /// Each name with no table, with the address of its expression, in the
    /// order the walk meets them.
    names: Vec<(usize, String)>,
    /// The addresses of the ORDER BY terms that name a result column.
    result_names: BTreeSet<usize>,
}

fn address(expr: &Expr<'_>) -> usize {
    std::ptr::from_ref(expr).addr()
}

impl Visit for BareNames {
    fn name(&mut self, expr: &Expr<'_>) {
        let name = match expr {
            Expr::Id(name) => name.0,
            Expr::Name(name) => name.0,
            _ => return,
        };
        if !name.eq_ignore_ascii_case("true") && !name.eq_ignore_ascii_case("false") {
            self.names.push((address(expr), name.to_owned()));
        }
    }

    fn select(
        &mut self,
        _scope: &Scope<'_, '_>,
        select: &Select<'_>,
        _first_row: bool,
        _reads: &Reads,
    ) -> rusqlite::Result<()> {
        // A single SELECT's ORDER BY term names a result column by the name
        // its AS gives; a compound's, which can name nothing else, also by
        // the name of the column an arm selects.
        let compounds = select.body.compounds.iter().flatten();
        let compound = select.body.compounds.is_some();
        let arms = std::iter::once(&select.body.select).chain(compounds.map(|arm| &arm.select));
        let columns = arms.filter_map(|arm| match arm {
            OneSelect::Select { columns, .. } => Some(columns.iter()),
            OneSelect::Values(_) => None,
        });
        let given: Vec<&str> = columns
            .flatten()
            .filter_map(|column| match column {
                ResultColumn::Expr(_, Some(As::As(alias) | As::Elided(alias))) => Some(alias.0),
                ResultColumn::Expr(Expr::Qualified(_, name), None) if compound => Some(name.0),
                _ => None,
            })
            .collect();
        for term in select.order_by.iter().flat_map(|terms| terms.iter()) {
            if let Expr::Id(name) = &term.expr {
                if given.iter().any(|given| Name(name.0) == Name(given)) {
                    self.result_names.insert(address(&term.expr));
                }
            }
        }
        Ok(())
    }

    fn from(
        &mut self,
        _scope: &Scope<'_, '_>,
        _from: &FromClause<'_>,
        items: &[&SelectTable<'_>],
    ) -> rusqlite::Result<()> {
        self.table_function |= items.iter().any(|item| matches!(item, SelectTable::TableCall(..)));
        Ok(())
    }
}
