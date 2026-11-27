//! The syntax of each of the app's views that one plan reads, parsed once, so
//! that every reading of the plan's SQL meets the same nodes: the planner
//! reads the view's FROM items, and `unreported_columns.rs` says which of
//! its SELECTs SQLite picks rows of by their addresses.

use std::cell::RefCell;
use std::collections::BTreeMap;

use sqlite3_parser::{
    ast::{Cmd, IndexedColumn, Select, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

use crate::schema_objects::SchemaLookups;

/// A view's column names, when its CREATE VIEW lists them, and its SELECT.
#[derive(Clone, Copy)]
pub(crate) struct ViewTree<'b> {
    pub(crate) columns: Option<&'b [IndexedColumn<'b>]>,
    pub(crate) select: &'b Select<'b>,
}

/// The views read so far, by their stored names, each with its syntax or the
/// parser's message when it can't read it.
pub(crate) struct ViewTrees<'b> {
    bump: &'b Bump,
    read: RefCell<BTreeMap<String, Result<ViewTree<'b>, String>>>,
}

impl<'b> ViewTrees<'b> {
    /// No view yet; each one read is kept in `bump`.
    pub(crate) fn new(bump: &'b Bump) -> Self {
        Self { bump, read: RefCell::new(BTreeMap::new()) }
    }

    /// The syntax of the view `stored` of `schema`, as the parser reads its
    /// CREATE VIEW, or the parser's message when it can't: SQLite and the
    /// parser can accept different language versions.
    pub(crate) fn tree(
        &self,
        schema: &SchemaLookups<'_>,
        stored: &str,
    ) -> rusqlite::Result<Result<ViewTree<'b>, String>> {
        if let Some(found) = self.read.borrow().get(stored) {
            return Ok(found.clone());
        }
        let sql = self.bump.alloc_str(schema.sql(stored)?);
        let mut parser = Parser::new(self.bump, sql.as_bytes());
        let found = match parser.next() {
            Ok(Some(Cmd::Stmt(Stmt::CreateView { columns, select, .. }))) => {
                Ok(ViewTree { columns, select })
            }
            Ok(_) => Err(format!("view {stored} holds no CREATE VIEW the parser reads")),
            Err(error) => Err(format!("view {stored}: {error}")),
        };
        self.read.borrow_mut().insert(stored.to_owned(), found.clone());
        Ok(found)
    }
}
