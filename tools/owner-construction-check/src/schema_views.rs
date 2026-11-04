//! The schema's views and what writing one writes (Appendix F.2). A view
//! holds no rows: an `INSERT`, `UPDATE` or `DELETE` on it runs the view's
//! `INSTEAD OF` triggers for that operation, so the write is theirs, and
//! where their bodies write a view in turn, that view's triggers' writes,
//! through every view on the way.
//!
//! The views are those the crates' production SQL creates, in string
//! literals and in `.sql` files; a temporary view belongs to its connection
//! and is none.

use std::collections::{BTreeMap, BTreeSet};

use crate::sql_statements::{statements, Creation, Operation};

/// A view's `INSTEAD OF` trigger for one operation: the writes of its body,
/// each a table or view and how it is written.
type TriggerWrites = Vec<(String, Operation)>;

#[derive(Default)]
pub(crate) struct SchemaViews {
    /// Each view the SQL creates, lowercase.
    views: BTreeSet<String>,
    /// The writes of each `INSTEAD OF` trigger, by its view and operation.
    triggers: BTreeMap<(String, Operation), Vec<TriggerWrites>>,
}

impl SchemaViews {
    /// The views of `texts`, each a piece of production SQL.
    pub(crate) fn collect<'a>(texts: impl IntoIterator<Item = &'a str>) -> Self {
        let mut views = Self::default();
        for text in texts {
            for statement in statements(text) {
                match statement.creation {
                    Some(Creation::View(view)) => {
                        views.views.insert(view);
                    }
                    Some(Creation::Trigger { table, instead_of: Some(operation) }) => {
                        views.triggers.entry((table, operation)).or_default().push(
                            statement
                                .writes
                                .into_iter()
                                .map(|write| (write.table, write.operation))
                                .collect(),
                        );
                    }
                    _ => {}
                }
            }
        }
        views
    }

    /// Whether the SQL creates the view `name`, lowercase.
    pub(crate) fn is_view(&self, name: &str) -> bool {
        self.views.contains(name)
    }

    /// The views the SQL creates, lowercase.
    pub(crate) fn names(&self) -> &BTreeSet<String> {
        &self.views
    }

    /// The tables writing `table` by `operation` writes: `table` itself, or,
    /// for a view, the tables its `INSTEAD OF` triggers for that operation
    /// write, through views in turn. A view written by an operation no
    /// trigger stands in for writes nothing.
    pub(crate) fn tables(&self, table: &str, operation: Operation) -> BTreeSet<String> {
        let mut tables = BTreeSet::new();
        self.expand(table, operation, &mut BTreeSet::new(), &mut tables);
        tables
    }

    /// The tables the view `view`'s `INSTEAD OF` triggers write, for any
    /// operation, through views in turn.
    pub(crate) fn tables_behind(&self, view: &str) -> BTreeSet<String> {
        let mut tables = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for operation in [Operation::Insert, Operation::Update, Operation::Delete] {
            self.expand(view, operation, &mut visited, &mut tables);
        }
        tables
    }

    fn expand(
        &self,
        table: &str,
        operation: Operation,
        visited: &mut BTreeSet<(String, Operation)>,
        tables: &mut BTreeSet<String>,
    ) {
        if !self.views.contains(table) {
            tables.insert(table.to_string());
            return;
        }
        if !visited.insert((table.to_string(), operation)) {
            return;
        }
        for writes in self.triggers.get(&(table.to_string(), operation)).into_iter().flatten() {
            for (written, operation) in writes {
                self.expand(written, *operation, visited, tables);
            }
        }
    }
}

#[cfg(test)]
#[path = "schema_views_tests.rs"]
mod tests;
