//! A statement keyed by what an earlier statement returned (the read rule):
//! read code that runs one statement, then runs a second with a value the
//! first gave, joins the two in Rust where one statement would read both.
//!
//! A statement is a call that hands a read value to its callee
//! (`ReadCode::hands_read_value`) or a SQL call on a connection or prepared
//! statement (`query_row`, `query`, `query_map`, `prepare`, `execute`). A
//! binding holds what a statement returned when its `let` (or `if let`)
//! runs a statement or names such a binding. A statement is keyed when an
//! argument, or the receiver of a call that hands over a read value, runs a
//! statement or names such a binding outside a closure: a closure argument
//! maps the rows the statement reads and does not choose them. A statement run once for each row is
//! `query-per-row` (`row_work.rs`) and is not counted again here.

use std::collections::BTreeSet;

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::policy::Shape;
use crate::read_code::{call_site, Body, ReadCode};
use crate::row_work::{Hit, ITERATOR_METHODS, ROW_CALLS, SQL_CALLS};

pub(crate) fn keyed_statements(read: &ReadCode<'_>) -> Vec<Hit> {
    let mut hits = Vec::new();
    for unit in read.units() {
        let mut visitor = Keyed {
            read,
            path: &unit.file.relative_path,
            item: &unit.item,
            returned: BTreeSet::new(),
            per_row: 0,
            hits: &mut hits,
        };
        match &unit.body {
            Body::Function { block, .. } => visitor.visit_block(block),
            Body::Closure(closure) => visitor.visit_expr(&closure.body),
        }
    }
    hits
}

struct Keyed<'a> {
    read: &'a ReadCode<'a>,
    path: &'a str,
    item: &'a str,
    /// The bindings that hold what a statement returned.
    returned: BTreeSet<String>,
    /// How many loop bodies, and closures run once for each row, the code
    /// visited is inside.
    per_row: usize,
    hits: &'a mut Vec<Hit>,
}

impl Keyed<'_> {
    /// What in `expr` holds what a statement returned: a binding it names,
    /// or a statement it runs.
    fn key_in(&self, expr: &syn::Expr) -> Option<String> {
        let mut found = Key { keyed: self, found: None };
        found.visit_expr(expr);
        found.found
    }

    /// The statement an expression is, by the name a finding gives it.
    fn statement(&self, expr: &syn::Expr) -> Option<String> {
        match expr {
            syn::Expr::Call(node) => self.call_statement(node),
            syn::Expr::MethodCall(node) => self.method_statement(node),
            _ => None,
        }
    }

    /// The callee a path call names, when the call hands it a read value.
    fn call_statement(&self, node: &syn::ExprCall) -> Option<String> {
        let syn::Expr::Path(path) = node.func.as_ref() else { return None };
        self.read
            .hands_read_value(self.path, call_site(node.span()))
            .then(|| path.path.segments.last().map(|last| last.ident.to_string()))
            .flatten()
    }

    /// The method, when it is a SQL call or the call hands it a read value.
    fn method_statement(&self, node: &syn::ExprMethodCall) -> Option<String> {
        let name = node.method.to_string();
        (SQL_CALLS.contains(&name.as_str())
            || self.read.hands_read_value(self.path, call_site(node.method.span())))
        .then_some(name)
    }

    fn bind(&mut self, pattern: &syn::Pat) {
        let mut names = Names(&mut self.returned);
        names.visit_pat(pattern);
    }

    /// Records a statement keyed by what `key` holds.
    fn keyed(&mut self, line: usize, statement: &str, key: &str) {
        self.hits.push(Hit {
            path: self.path.to_string(),
            line,
            item: self.item.to_string(),
            shape: Shape::KeyedStatement,
            what: format!("runs `{statement}`, which runs SQL, keyed by what `{key}` returned"),
        });
    }

    /// Records the statement at `line` when one of `keys` holds what an
    /// earlier statement returned, unless it runs once for each row.
    fn check(&mut self, line: usize, statement: Option<String>, keys: &[Option<String>]) {
        if self.per_row > 0 {
            return;
        }
        if let (Some(statement), Some(key)) = (statement, keys.iter().flatten().next()) {
            self.keyed(line, &statement, key);
        }
    }
}

/// The first binding holding what a statement returned that an expression
/// names, or the first statement it runs.
struct Key<'a, 'b> {
    keyed: &'a Keyed<'b>,
    found: Option<String>,
}

impl<'ast> Visit<'ast> for Key<'_, '_> {
    fn visit_expr(&mut self, node: &'ast syn::Expr) {
        if self.found.is_some() {
            return;
        }
        if let syn::Expr::Path(path) = node {
            if let Some(name) = path.path.get_ident() {
                if self.keyed.returned.contains(&name.to_string()) {
                    self.found = Some(name.to_string());
                    return;
                }
            }
        }
        if let Some(statement) = self.keyed.statement(node) {
            self.found = Some(statement);
            return;
        }
        visit::visit_expr(self, node);
    }

    // A closure argument maps the rows a statement reads; it does not choose
    // them.
    fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}

    // What an assignment writes is no value it reads, as the name of a
    // `format!` argument (`artists = listing()`) is not.
    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        self.visit_expr(&node.right);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

struct Names<'a>(&'a mut BTreeSet<String>);

impl<'ast> Visit<'ast> for Names<'_> {
    fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
        self.0.insert(node.ident.to_string());
        visit::visit_pat_ident(self, node);
    }
}

impl<'ast> Visit<'ast> for Keyed<'_> {
    // A nested item is read code only when something reads it.
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_local(&mut self, node: &'ast syn::Local) {
        visit::visit_local(self, node);
        if node.init.as_ref().is_some_and(|init| self.key_in(&init.expr).is_some()) {
            self.bind(&node.pat);
        }
    }

    fn visit_expr_let(&mut self, node: &'ast syn::ExprLet) {
        visit::visit_expr_let(self, node);
        if self.key_in(&node.expr).is_some() {
            self.bind(&node.pat);
        }
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        self.per_row += 1;
        self.visit_block(&node.body);
        self.per_row -= 1;
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.per_row += 1;
        visit::visit_expr_while(self, node);
        self.per_row -= 1;
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.per_row += 1;
        visit::visit_expr_loop(self, node);
        self.per_row -= 1;
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let Some(statement) = self.call_statement(node) {
            let keys = node.args.iter().map(|argument| self.key_in(argument)).collect::<Vec<_>>();
            self.check(node.span().start().line, Some(statement), &keys);
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let name = node.method.to_string();
        if let Some(statement) = self.method_statement(node) {
            // A SQL call's receiver is the connection or statement it runs on.
            let receiver = (!SQL_CALLS.contains(&name.as_str()))
                .then(|| self.key_in(&node.receiver))
                .flatten();
            let mut keys = vec![receiver];
            keys.extend(node.args.iter().map(|argument| self.key_in(argument)));
            self.check(node.method.span().start().line, Some(statement), &keys);
        }
        self.visit_expr(&node.receiver);
        let per_row =
            ITERATOR_METHODS.contains(&name.as_str()) || ROW_CALLS.contains(&name.as_str());
        for argument in &node.args {
            let closure = matches!(argument, syn::Expr::Closure(_));
            self.per_row += usize::from(per_row && closure);
            self.visit_expr(argument);
            self.per_row -= usize::from(per_row && closure);
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "keyed_statements_tests.rs"]
mod tests;
