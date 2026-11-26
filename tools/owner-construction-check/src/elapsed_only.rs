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
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if node.method == "elapsed" {
            self.starts.extend(self.measured_call(&node.receiver));
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        for (index, statement) in node.stmts.iter().enumerate() {
            let syn::Stmt::Local(local) = statement else { continue };
            let (syn::Pat::Ident(name), Some(init)) = (&local.pat, &local.init) else { continue };
            let Some(start) = self.measured_call(&init.expr) else { continue };
            let mut uses = Uses { name: name.ident.to_string(), other: 0 };
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

/// How code uses the name a measured value is bound to: it counts every use
/// but asking `.elapsed()` for a log line or a work count. The duration
/// decides behavior when a comparison, a condition or a `match` reads it, so
/// any use of the name there counts too.
struct Uses {
    name: String,
    /// Uses that are no plain `.elapsed()`.
    other: usize,
}

impl Uses {
    fn is_name(&self, expr: &syn::Expr) -> bool {
        matches!(expr, syn::Expr::Path(path) if path.path.is_ident(&self.name))
    }

    /// Counts the uses of the name in macro arguments that do not parse as
    /// code: the name followed by `.elapsed` is no other use.
    fn count_tokens(&mut self, tokens: TokenStream) {
        let tokens = tokens.into_iter().collect::<Vec<_>>();
        for (index, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Ident(ident) if *ident == self.name => {
                    let asks_elapsed = matches!(
                        (tokens.get(index + 1), tokens.get(index + 2)),
                        (Some(TokenTree::Punct(dot)), Some(TokenTree::Ident(method)))
                            if dot.as_char() == '.' && method == "elapsed"
                    );
                    if !asks_elapsed {
                        self.other += 1;
                    }
                }
                TokenTree::Group(group) => self.count_tokens(group.stream()),
                _ => {}
            }
        }
    }
}

impl Uses {
    /// Counts one other use when `expr` mentions the name at all.
    fn decides(&mut self, expr: &syn::Expr) {
        let mut mentions = Mentions { name: &self.name, found: false };
        mentions.visit_expr(expr);
        if mentions.found {
            self.other += 1;
        }
    }
}

/// Whether an expression names `name` anywhere, in code or in macro tokens.
struct Mentions<'a> {
    name: &'a str,
    found: bool,
}

impl<'ast> Visit<'ast> for Mentions<'_> {
    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        self.found |= node.path.is_ident(self.name);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        self.found |= node.tokens.to_string().contains(self.name);
    }
}

impl<'ast> Visit<'ast> for Uses {
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if !(node.method == "elapsed" && node.args.is_empty() && self.is_name(&node.receiver)) {
            visit::visit_expr_method_call(self, node);
        }
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        self.decides(&node.left);
        self.decides(&node.right);
    }

    fn visit_expr_if(&mut self, node: &'ast syn::ExprIf) {
        self.decides(&node.cond);
        visit::visit_block(self, &node.then_branch);
        if let Some((_, otherwise)) = &node.else_branch {
            self.visit_expr(otherwise);
        }
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        self.decides(&node.cond);
        visit::visit_block(self, &node.body);
    }

    fn visit_expr_match(&mut self, node: &'ast syn::ExprMatch) {
        self.decides(&node.expr);
        for arm in &node.arms {
            self.visit_arm(arm);
        }
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if node.path.is_ident(&self.name) {
            self.other += 1;
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => self.count_tokens(unparsed_tokens(node)),
        }
    }
}

#[cfg(test)]
#[path = "elapsed_only_tests.rs"]
mod tests;
