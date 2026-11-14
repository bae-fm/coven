//! Cutoffs between read paths (the read rule): a read has one path, so it
//! compares nothing against a limit to pick between two. In read code, a
//! relational comparison (`<`, `<=`, `>`, `>=`, also as a `match` scrutinee)
//! with an operand that names an integer `const` of the workspace, holds an
//! integer literal of 2 or more, or calls a workspace function whose body
//! names an integer `const`. Comparing with 0 or 1, as emptiness is, is none.

use std::collections::BTreeSet;

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::policy::Shape;
use crate::read_code::{call_site, Body, ReadCode};
use crate::row_work::Hit;
use crate::syntax::RustFile;

const INTEGERS: [&str; 12] =
    ["u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize"];

/// Each cutoff in read code.
pub(crate) fn cutoffs(read: &ReadCode<'_>, files: &[RustFile]) -> Vec<Hit> {
    let mut found = Constants::default();
    for file in files {
        found.visit_file(&file.syntax);
    }
    let constants = found.names;
    let naming = read
        .index()
        .callables
        .iter()
        .map(|callable| {
            callable.body.and_then(|body| {
                let mut names = ConstantNames { constants: &constants, found: None };
                names.visit_block(body);
                names.found
            })
        })
        .collect::<Vec<_>>();
    let mut hits = Vec::new();
    for unit in read.units() {
        let mut visitor = Comparisons {
            read,
            constants: &constants,
            naming: &naming,
            path: &unit.file.relative_path,
            item: &unit.item,
            hits: &mut hits,
        };
        match &unit.body {
            Body::Function { block, .. } => visitor.visit_block(block),
            Body::Closure(closure) => visitor.visit_expr(&closure.body),
        }
    }
    hits
}

/// The integer constants the workspace declares, by name.
#[derive(Default)]
struct Constants {
    names: BTreeSet<String>,
}

impl Constants {
    fn add(&mut self, name: &syn::Ident, ty: &syn::Type) {
        let integer = matches!(ty, syn::Type::Path(path)
            if path.path.get_ident().is_some_and(|ident| INTEGERS.contains(&ident.to_string().as_str())));
        if integer {
            self.names.insert(name.to_string());
        }
    }
}

impl<'ast> Visit<'ast> for Constants {
    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        self.add(&node.ident, &node.ty);
        visit::visit_item_const(self, node);
    }

    fn visit_impl_item_const(&mut self, node: &'ast syn::ImplItemConst) {
        self.add(&node.ident, &node.ty);
        visit::visit_impl_item_const(self, node);
    }
}

/// The first integer constant a body names.
struct ConstantNames<'a> {
    constants: &'a BTreeSet<String>,
    found: Option<String>,
}

impl<'ast> Visit<'ast> for ConstantNames<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(name) = node.path.segments.last().map(|segment| segment.ident.to_string()) {
            if self.found.is_none() && self.constants.contains(&name) {
                self.found = Some(name);
            }
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

struct Comparisons<'a> {
    read: &'a ReadCode<'a>,
    constants: &'a BTreeSet<String>,
    /// For each function of the index, the constant its body names.
    naming: &'a [Option<String>],
    path: &'a str,
    item: &'a str,
    hits: &'a mut Vec<Hit>,
}

/// What fixes an operand: the first constant, literal or function naming a
/// constant it holds.
struct Fixed<'a, 'b> {
    comparisons: &'a Comparisons<'b>,
    found: Option<String>,
}

impl<'ast> Visit<'ast> for Fixed<'_, '_> {
    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        if let Some(name) = node.path.segments.last().map(|segment| segment.ident.to_string()) {
            if self.found.is_none() && self.comparisons.constants.contains(&name) {
                self.found = Some(format!("the constant {name}"));
            }
        }
    }

    fn visit_expr_lit(&mut self, node: &'ast syn::ExprLit) {
        if let syn::Lit::Int(number) = &node.lit {
            if self.found.is_none() && number.base10_parse::<u128>().is_ok_and(|value| value >= 2) {
                self.found = Some(format!("the literal {}", number.base10_digits()));
            }
        }
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        let callee = match node.func.as_ref() {
            syn::Expr::Path(path) => {
                path.path.segments.last().map(|segment| segment.ident.to_string())
            }
            _ => None,
        };
        self.call(call_site(node.span()), callee);
        visit::visit_expr_call(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        self.call(call_site(node.method.span()), Some(node.method.to_string()));
        visit::visit_expr_method_call(self, node);
    }
}

impl Fixed<'_, '_> {
    fn call(&mut self, site: crate::read_code::CallSite, callee: Option<String>) {
        if self.found.is_some() {
            return;
        }
        let comparisons = self.comparisons;
        let named = comparisons
            .read
            .callees(comparisons.path, site)
            .iter()
            .find_map(|&target| comparisons.naming.get(target).cloned().flatten());
        if let (Some(constant), Some(callee)) = (named, callee) {
            self.found = Some(format!("the constant {constant} in `{callee}`"));
        }
    }
}

impl<'ast> Visit<'ast> for Comparisons<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        let operator = match node.op {
            syn::BinOp::Lt(_) => Some("<"),
            syn::BinOp::Le(_) => Some("<="),
            syn::BinOp::Gt(_) => Some(">"),
            syn::BinOp::Ge(_) => Some(">="),
            _ => None,
        };
        if let Some(operator) = operator {
            let mut fixed = None;
            for operand in [&node.left, &node.right] {
                let mut search = Fixed { comparisons: self, found: None };
                search.visit_expr(operand);
                fixed = fixed.or(search.found);
            }
            if let Some(fixed) = fixed {
                self.hits.push(Hit {
                    path: self.path.to_string(),
                    line: node.span().start().line,
                    item: self.item.to_string(),
                    shape: Shape::Cutoff,
                    what: format!("compares with `{operator}` against {fixed}"),
                });
            }
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "cutoffs_tests.rs"]
mod tests;
