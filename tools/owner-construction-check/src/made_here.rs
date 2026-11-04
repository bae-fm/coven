//! Whether a method hands out a value it made or one it took from its owner
//! (Appendix F.3): an owner may return, or give its caller's callback, a new
//! value of a type it also holds, such as a fresh cancellation token, but
//! not the one it holds. The body tells them apart: a value is made in the
//! method when the expressions handing it out read nothing of `self`, by
//! themselves or through a local bound from what does. A macro's value is
//! unknown to the checker, and so is a closure's argument, so either counts
//! as read from the owner; so does every value of a method with no body.

use std::collections::BTreeSet;

use syn::visit::{self, Visit};

use crate::macros::{parse_macro_body, token_paths, unparsed_tokens};

/// Whether every value `body` returns is made in it.
pub(crate) fn returns_made_here(body: Option<&syn::Block>) -> bool {
    let Some(body) = body else {
        return false;
    };
    let read = OwnerReads::of(body);
    let mut returns = Returns(Vec::new());
    returns.visit_block(body);
    let tail = match body.stmts.last() {
        Some(syn::Stmt::Expr(expression, None)) => Some(expression),
        _ => None,
    };
    tail.into_iter().chain(returns.0).all(|expression| !read.reads(expression))
}

/// Whether every value `body` gives as argument `index` of its callback
/// parameter `parameter` is made in it. A callback the body does anything
/// with but call, such as pass on, may be given anything.
pub(crate) fn callback_argument_made_here(
    body: Option<&syn::Block>,
    parameter: Option<&str>,
    index: usize,
) -> bool {
    let (Some(body), Some(parameter)) = (body, parameter) else {
        return false;
    };
    let read = OwnerReads::of(body);
    let mut calls = CallbackCalls { parameter, arguments: Vec::new(), passed_on: false };
    calls.visit_block(body);
    !calls.passed_on
        && calls
            .arguments
            .iter()
            .all(|arguments| arguments.get(index).is_some_and(|argument| !read.reads(argument)))
}

/// The locals of a body bound from what reads the owner.
struct OwnerReads {
    locals: BTreeSet<String>,
}

impl OwnerReads {
    /// Each binding's names read the owner when what they are bound from
    /// does; a name bound more than once reads it when any binding does.
    fn of(body: &syn::Block) -> Self {
        let mut bindings = Bindings(Vec::new());
        bindings.visit_block(body);
        let mut read = OwnerReads { locals: BTreeSet::new() };
        loop {
            let before = read.locals.len();
            for (names, source) in &bindings.0 {
                if source.is_none_or(|source| read.reads(source)) {
                    read.locals.extend(names.iter().cloned());
                }
            }
            if read.locals.len() == before {
                return read;
            }
        }
    }

    fn reads(&self, expression: &syn::Expr) -> bool {
        let mut reads = Reads { locals: &self.locals, found: false };
        reads.visit_expr(expression);
        reads.found
    }
}

struct Reads<'a> {
    locals: &'a BTreeSet<String>,
    found: bool,
}

impl<'ast> Visit<'ast> for Reads<'_> {
    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if node.qself.is_none()
            && node
                .path
                .get_ident()
                .is_some_and(|name| name == "self" || self.locals.contains(&name.to_string()))
        {
            self.found = true;
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_macro(&mut self, _: &'ast syn::Macro) {
        self.found = true;
    }
}

/// Each pattern's names with what they are bound from; `None` for a
/// closure's arguments, which its caller gives.
struct Bindings<'ast>(Vec<(BTreeSet<String>, Option<&'ast syn::Expr>)>);

impl<'ast> Bindings<'ast> {
    fn bind(&mut self, pattern: &'ast syn::Pat, source: Option<&'ast syn::Expr>) {
        let mut names = Names(BTreeSet::new());
        names.visit_pat(pattern);
        self.0.push((names.0, source));
    }
}

impl<'ast> Visit<'ast> for Bindings<'ast> {
    fn visit_local(&mut self, node: &'ast syn::Local) {
        self.bind(&node.pat, node.init.as_ref().map(|init| init.expr.as_ref()));
        visit::visit_local(self, node);
    }

    fn visit_expr_let(&mut self, node: &'ast syn::ExprLet) {
        self.bind(&node.pat, Some(&node.expr));
        visit::visit_expr_let(self, node);
    }

    fn visit_expr_match(&mut self, node: &'ast syn::ExprMatch) {
        for arm in &node.arms {
            self.bind(&arm.pat, Some(&node.expr));
        }
        visit::visit_expr_match(self, node);
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.bind(&node.pat, Some(&node.expr));
        visit::visit_expr_for_loop(self, node);
    }

    fn visit_expr_closure(&mut self, node: &'ast syn::ExprClosure) {
        for input in &node.inputs {
            self.bind(input, None);
        }
        visit::visit_expr_closure(self, node);
    }

    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        if let syn::Expr::Path(path) = node.left.as_ref() {
            if let Some(name) = path.path.get_ident() {
                self.0.push((BTreeSet::from([name.to_string()]), Some(&node.right)));
            }
        }
        visit::visit_expr_assign(self, node);
    }
}

struct Names(BTreeSet<String>);

impl<'ast> Visit<'ast> for Names {
    fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
        self.0.insert(node.ident.to_string());
        visit::visit_pat_ident(self, node);
    }
}

/// The `return` expressions of a body, outside the closures and async
/// blocks inside it, whose `return` ends them instead.
struct Returns<'ast>(Vec<&'ast syn::Expr>);

impl<'ast> Visit<'ast> for Returns<'ast> {
    fn visit_expr_return(&mut self, node: &'ast syn::ExprReturn) {
        if let Some(expression) = &node.expr {
            self.0.push(expression);
        }
        visit::visit_expr_return(self, node);
    }

    fn visit_expr_closure(&mut self, _: &'ast syn::ExprClosure) {}

    fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}
}

/// Each call of the callback `parameter`, and whether the body uses it any
/// other way.
struct CallbackCalls<'p, 'ast> {
    parameter: &'p str,
    arguments: Vec<Vec<&'ast syn::Expr>>,
    passed_on: bool,
}

impl<'ast> Visit<'ast> for CallbackCalls<'_, 'ast> {
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        let called = matches!(node.func.as_ref(), syn::Expr::Path(path)
            if path.qself.is_none() && path.path.is_ident(self.parameter));
        if called {
            self.arguments.push(node.args.iter().collect());
        } else {
            self.visit_expr(&node.func);
        }
        for argument in &node.args {
            self.visit_expr(argument);
        }
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if node.qself.is_none() && node.path.is_ident(self.parameter) {
            self.passed_on = true;
        }
        visit::visit_expr_path(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.passed_on |= names_value(node, self.parameter);
    }
}

/// Whether a macro's arguments name `parameter` as a value, a call of it
/// included: what a macro gives a callback it calls is unknown, so it may be
/// anything. A field name before `=`, as in `tracing::error!(name = value)`,
/// is no value of it.
fn names_value(node: &syn::Macro, parameter: &str) -> bool {
    match parse_macro_body(node) {
        Some(body) => {
            let mut values = Values { parameter, found: false };
            body.visit(&mut values);
            values.found
        }
        None => token_paths(unparsed_tokens(node)).iter().any(|path| {
            !path.after_dot
                && !path.leading_colon
                && !path.type_position
                && !path.assigned
                && path.segments == [parameter]
        }),
    }
}

/// Whether parsed macro arguments name `parameter` as a value.
struct Values<'p> {
    parameter: &'p str,
    found: bool,
}

impl<'ast> Visit<'ast> for Values<'_> {
    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if node.qself.is_none() && node.path.is_ident(self.parameter) {
            self.found = true;
        }
        visit::visit_expr_path(self, node);
    }

    /// What an assignment writes to, a name or its fields, is no value.
    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        if !is_place(&node.left) {
            self.visit_expr(&node.left);
        }
        self.visit_expr(&node.right);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.found |= names_value(node, self.parameter);
    }
}

/// A name, or a field of one at any depth.
fn is_place(expression: &syn::Expr) -> bool {
    match expression {
        syn::Expr::Path(path) => path.qself.is_none() && path.path.get_ident().is_some(),
        syn::Expr::Field(field) => is_place(&field.base),
        _ => false,
    }
}
