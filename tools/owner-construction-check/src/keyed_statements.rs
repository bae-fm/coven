//! This check finds a statement keyed by what an earlier statement returned
//! (the read rule). Read code that runs one statement and then runs a second
//! with a value the first returned joins the two in Rust, where one
//! statement would read both.
//!
//! A statement is a call that runs SQL (`read_statements.rs`): a SQL call on
//! a connection or prepared statement, or a call that hands its callee a
//! read value. A binding holds what a statement returned when its `let` (or
//! `if let`) runs a statement or names such a binding, and so does a `match`
//! arm's pattern when the matched expression does. The check reports a
//! statement when an argument, or the receiver of a call that hands over a
//! read value, runs a statement or names such a binding outside a closure: a
//! closure argument maps the rows the statement reads and does not choose
//! them. `row_work.rs` reports a statement that runs once for each row as
//! `query-per-row`, and this check does not report it again.

use std::collections::BTreeSet;

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::policy::Shape;
use crate::read_code::{Body, ReadCode};
use crate::read_statements::{RowDepth, Statement, VisitRows};
use crate::row_work::Hit;

pub(crate) fn keyed_statements(read: &ReadCode<'_>) -> Vec<Hit> {
    let mut hits = Vec::new();
    for unit in read.units() {
        let mut visitor = Keyed {
            read,
            path: &unit.file.relative_path,
            item: &unit.item,
            returned: BTreeSet::new(),
            depth: RowDepth::default(),
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
    depth: RowDepth,
    hits: &'a mut Vec<Hit>,
}

impl Keyed<'_> {
    /// What in `expr` holds what a statement returned: a binding it names,
    /// or a statement it runs.
    fn key_in(&self, expr: &syn::Expr) -> Option<KeySource> {
        let mut found = Key { keyed: self, found: None };
        found.visit_expr(expr);
        found.found
    }

    /// The statement an expression runs, by the name a finding gives it.
    fn statement(&self, expr: &syn::Expr) -> Option<String> {
        let statement = match expr {
            syn::Expr::Call(node) => Statement::of_call(self.read, self.path, node),
            syn::Expr::MethodCall(node) => Statement::of_method(self.read, self.path, node),
            _ => None,
        };
        statement.map(|statement| statement.name().to_string())
    }

    fn bind(&mut self, pattern: &syn::Pat) {
        let mut names = Names(&mut self.returned);
        names.visit_pat(pattern);
    }

    /// Records a statement keyed by what `key` holds or returns.
    fn keyed(&mut self, line: usize, statement: &str, key: &KeySource) {
        let key = match key {
            KeySource::Binding(name) => format!("what `{name}` holds"),
            KeySource::Statement(name) => format!("what `{name}` returns"),
        };
        self.hits.push(Hit {
            path: self.path.to_string(),
            line,
            item: self.item.to_string(),
            shape: Shape::KeyedStatement,
            what: format!("runs `{statement}`, which runs SQL, keyed by {key}"),
        });
    }

    /// Records the statement at `line` when one of `keys` holds what an
    /// earlier statement returned, unless it runs once for each row.
    fn check(&mut self, line: usize, statement: &str, keys: &[Option<KeySource>]) {
        if self.depth.per_row() {
            return;
        }
        if let Some(key) = keys.iter().flatten().next() {
            self.keyed(line, statement, key);
        }
    }
}

/// What holds a key: a binding that holds what a statement returned, or a
/// statement that returns it.
enum KeySource {
    Binding(String),
    Statement(String),
}

/// A visitor that finds, in an expression, the first binding that holds what
/// a statement returned, or the first statement the expression runs.
struct Key<'a, 'b> {
    keyed: &'a Keyed<'b>,
    found: Option<KeySource>,
}

impl<'ast> Visit<'ast> for Key<'_, '_> {
    fn visit_expr(&mut self, node: &'ast syn::Expr) {
        if self.found.is_some() {
            return;
        }
        if let syn::Expr::Path(path) = node {
            if let Some(name) = path.path.get_ident() {
                if self.keyed.returned.contains(&name.to_string()) {
                    self.found = Some(KeySource::Binding(name.to_string()));
                    return;
                }
            }
        }
        if let Some(statement) = self.keyed.statement(node) {
            self.found = Some(KeySource::Statement(statement));
            return;
        }
        visit::visit_expr(self, node);
    }

    // A closure argument maps the rows a statement reads; it does not choose
    // them.
    fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}

    // Only the right side of an assignment reads a value. The left side, as
    // the name of a `format!` argument (`artists = listing()`), reads
    // nothing.
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

    fn visit_expr_match(&mut self, node: &'ast syn::ExprMatch) {
        self.visit_expr(&node.expr);
        if self.key_in(&node.expr).is_some() {
            for arm in &node.arms {
                self.bind(&arm.pat);
            }
        }
        for arm in &node.arms {
            self.visit_arm(arm);
        }
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_for_loop_rows(node);
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.visit_while_rows(node);
    }

    fn visit_expr_loop(&mut self, node: &'ast syn::ExprLoop) {
        self.visit_loop_rows(node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let Some(statement) = Statement::of_call(self.read, self.path, node) {
            let keys = node.args.iter().map(|argument| self.key_in(argument)).collect::<Vec<_>>();
            self.check(node.span().start().line, statement.name(), &keys);
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if let Some(statement) = Statement::of_method(self.read, self.path, node) {
            // A SQL call's receiver is the connection or statement it runs on.
            let receiver = match statement {
                Statement::Sql(_) => None,
                Statement::Read(_) => self.key_in(&node.receiver),
            };
            let mut keys = vec![receiver];
            keys.extend(node.args.iter().map(|argument| self.key_in(argument)));
            self.check(node.method.span().start().line, statement.name(), &keys);
        }
        self.visit_method_call_rows(node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

impl<'ast> VisitRows<'ast> for Keyed<'_> {
    fn depth(&mut self) -> &mut RowDepth {
        &mut self.depth
    }
}

#[cfg(test)]
#[path = "keyed_statements_tests.rs"]
mod tests;
