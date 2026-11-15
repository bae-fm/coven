//! The statements read code runs, and the loops and closures that run code
//! once for each row. Two checks ask both questions: `row_work.rs` reports a
//! statement that runs once for each row (`query-per-row`), and
//! `keyed_statements.rs` reports a statement whose key an earlier statement
//! returned.

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::read_code::{call_site, ReadCode};

/// The SQL calls on a connection or prepared statement.
const SQL_CALLS: [&str; 5] = ["query", "query_row", "prepare", "query_map", "execute"];
/// Methods that run their closure argument once for each row.
const ITERATOR_METHODS: [&str; 12] = [
    "map",
    "for_each",
    "filter_map",
    "flat_map",
    "try_for_each",
    "fold",
    "try_fold",
    "inspect",
    "scan",
    "filter",
    "find_map",
    "map_while",
];
/// The calls whose closure argument runs once for each row they read.
const ROW_CALLS: [&str; 3] = ["query", "query_row", "query_map"];

/// A call that runs SQL.
pub(crate) enum Statement {
    /// A SQL call on a connection or prepared statement, by its method name.
    Sql(String),
    /// A call that hands a read value to its callee
    /// (`ReadCode::hands_read_value`), by the callee's name.
    Read(String),
}

impl Statement {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Sql(name) | Self::Read(name) => name,
        }
    }

    /// The statement a call through a path runs, when the call hands its
    /// callee a read value.
    pub(crate) fn of_call(read: &ReadCode<'_>, path: &str, node: &syn::ExprCall) -> Option<Self> {
        let syn::Expr::Path(callee) = node.func.as_ref() else { return None };
        let name = callee.path.segments.last()?.ident.to_string();
        read.hands_read_value(path, call_site(node.span())).then_some(Self::Read(name))
    }

    /// The statement a method call runs: a SQL call, or a call that hands
    /// the method a read value.
    pub(crate) fn of_method(
        read: &ReadCode<'_>,
        path: &str,
        node: &syn::ExprMethodCall,
    ) -> Option<Self> {
        let name = node.method.to_string();
        if SQL_CALLS.contains(&name.as_str()) {
            Some(Self::Sql(name))
        } else {
            read.hands_read_value(path, call_site(node.method.span())).then_some(Self::Read(name))
        }
    }
}

/// How many loop bodies, and how many closures that run once for each row,
/// enclose the code a visitor is in.
#[derive(Default)]
pub(crate) struct RowDepth {
    loops: usize,
    rows: usize,
}

impl RowDepth {
    /// Whether a loop body encloses the code.
    pub(crate) fn in_loop(&self) -> bool {
        self.loops > 0
    }

    /// Whether the code runs once for each row: a loop body or a closure
    /// that runs once for each row encloses it.
    pub(crate) fn per_row(&self) -> bool {
        self.loops > 0 || self.rows > 0
    }
}

/// A visitor that counts the loop bodies and per-row closures around the
/// code it visits. Its `Visit` methods for loops and method calls call
/// these methods.
pub(crate) trait VisitRows<'ast>: Visit<'ast> + Sized {
    fn depth(&mut self) -> &mut RowDepth;

    fn visit_for_loop_rows(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        self.depth().loops += 1;
        self.visit_block(&node.body);
        self.depth().loops -= 1;
    }

    fn visit_while_rows(&mut self, node: &'ast syn::ExprWhile) {
        self.depth().loops += 1;
        visit::visit_expr_while(self, node);
        self.depth().loops -= 1;
    }

    fn visit_loop_rows(&mut self, node: &'ast syn::ExprLoop) {
        self.depth().loops += 1;
        visit::visit_expr_loop(self, node);
        self.depth().loops -= 1;
    }

    /// Visits a method call's receiver and arguments. A closure argument of
    /// an iterator method or a SQL row call runs once for each row.
    fn visit_method_call_rows(&mut self, node: &'ast syn::ExprMethodCall) {
        self.visit_expr(&node.receiver);
        let name = node.method.to_string();
        let per_row =
            ITERATOR_METHODS.contains(&name.as_str()) || ROW_CALLS.contains(&name.as_str());
        for argument in &node.args {
            let closure = per_row && matches!(argument, syn::Expr::Closure(_));
            self.depth().rows += usize::from(closure);
            self.visit_expr(argument);
            self.depth().rows -= usize::from(closure);
        }
    }
}
