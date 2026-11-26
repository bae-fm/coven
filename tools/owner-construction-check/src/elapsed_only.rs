//! The `measured_paths` of a capability's gates: a function that reads a
//! monotonic clock, such as `Instant::now`. Its value measures a duration
//! when code only asks it `elapsed`, so a log line or a work count may hold
//! it. A value that reaches anything else, a branch, a field or a call, can
//! decide behavior or reach stored data, and stays a use of the capability.

use std::collections::BTreeSet;

use proc_macro2::{TokenStream, TokenTree};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::capability_boundaries::holds_any;
use crate::macros::{parse_macro_body, unparsed_tokens};
use crate::name_resolution::qualified_path;

/// Where a path starts: its line and column.
pub(crate) type Start = (usize, usize);

/// The starts of the calls of `patterns` in `file` whose value only measures
/// an elapsed duration: a call that `.elapsed()` is asked of at once, or one
/// bound to a name that the rest of its block asks `.elapsed()` of and
/// uses no other way.
pub(crate) fn measuring_starts(file: &syn::File, patterns: &[Vec<String>]) -> BTreeSet<Start> {
    let mut starts = Starts { patterns, starts: BTreeSet::new() };
    starts.visit_file(file);
    starts.starts
}

struct Starts<'a> {
    patterns: &'a [Vec<String>],
    starts: BTreeSet<Start>,
}

impl Starts<'_> {
    /// The start of the path of `expr` when it calls a measured function
    /// with no arguments.
    fn measured_call(&self, expr: &syn::Expr) -> Option<Start> {
        let syn::Expr::Call(call) = expr else { return None };
        let syn::Expr::Path(path) = call.func.as_ref() else { return None };
        let segments = qualified_path(&path.path);
        let measured = holds_any(&segments, self.patterns);
        let start = path.path.span().start();
        (measured && call.args.is_empty()).then_some((start.line, start.column))
    }
}

impl<'ast> Visit<'ast> for Starts<'_> {
    fn visit_block(&mut self, node: &'ast syn::Block) {
        for (index, statement) in node.stmts.iter().enumerate() {
            let syn::Stmt::Local(local) = statement else { continue };
            let (syn::Pat::Ident(name), Some(init)) = (&local.pat, &local.init) else { continue };
            let Some(start) = self.measured_call(&init.expr) else { continue };
            let mut uses = Uses { tracked: BTreeSet::from([name.ident.to_string()]), other: 0 };
            for later in &node.stmts[index + 1..] {
                uses.visit_stmt(later);
            }
            if uses.other == 0 {
                self.starts.insert(start);
            }
        }
        visit::visit_block(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

/// The macros that only write a value out: a log line or a print.
const WRITING_MACROS: [&str; 8] =
    ["trace", "debug", "info", "warn", "error", "event", "println", "eprintln"];

/// How code uses a measured value and the durations taken from it. A tracked
/// name may be asked `.elapsed()` and its duration may be bound to a name,
/// written by a log macro, or added into a count (`count += duration`). Every
/// other mention, a comparison, a condition, a call, a field or a return,
/// counts in `other`: it can decide behavior or reach stored data.
struct Uses {
    tracked: BTreeSet<String>,
    /// Mentions of a tracked name where it may not appear.
    other: usize,
}

impl Uses {
    fn tracks(&self, expr: &syn::Expr) -> bool {
        matches!(expr, syn::Expr::Path(path)
            if path.path.get_ident().is_some_and(|name| self.tracked.contains(&name.to_string())))
    }

    /// Whether `expr` is a tracked name's duration or a tracked duration.
    fn is_duration(&self, expr: &syn::Expr) -> bool {
        match expr {
            syn::Expr::MethodCall(call) => {
                call.method == "elapsed" && call.args.is_empty() && self.tracks(&call.receiver)
            }
            _ => self.tracks(expr),
        }
    }

    /// Counts the tracked names in macro arguments that do not parse as code.
    fn count_tokens(&mut self, tokens: TokenStream) {
        for token in tokens {
            match token {
                TokenTree::Ident(ident) if self.tracked.contains(&ident.to_string()) => {
                    self.other += 1;
                }
                TokenTree::Group(group) => self.count_tokens(group.stream()),
                _ => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for Uses {
    fn visit_local(&mut self, node: &'ast syn::Local) {
        let duration = node.init.as_ref().is_some_and(|init| self.is_duration(&init.expr));
        match (&node.pat, duration) {
            (syn::Pat::Ident(name), true) => {
                self.tracked.insert(name.ident.to_string());
            }
            _ => visit::visit_local(self, node),
        }
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        let adds = matches!(node.op, syn::BinOp::AddAssign(_) | syn::BinOp::SubAssign(_));
        self.visit_expr(&node.left);
        if !(adds && self.is_duration(&node.right)) {
            self.visit_expr(&node.right);
        }
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if self.tracks(&syn::Expr::Path(node.clone())) {
            self.other += 1;
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let writes = node
            .path
            .segments
            .last()
            .is_some_and(|name| WRITING_MACROS.iter().any(|writing| name.ident == writing));
        if !writes {
            match parse_macro_body(node) {
                Some(body) => body.visit(self),
                None => self.count_tokens(unparsed_tokens(node)),
            }
        }
    }
}

#[cfg(test)]
#[path = "elapsed_only_tests.rs"]
mod tests;
