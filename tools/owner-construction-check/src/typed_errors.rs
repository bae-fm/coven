//! Errors are typed (Appendix F.2). Production code never makes an error a
//! string: no function returns `Result<_, String>`, and no `map_err` turns
//! its error into text with `to_string()` or `format!`. A string error can't
//! be matched on, loses its cause, and leaves every caller reading prose to
//! decide what to do.

use std::collections::BTreeSet;

use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::syntax::RustFile;

const REMEDY: &str =
    "give the failure an error type, with its cause as the source, and return that";

pub(crate) fn find_string_error_violations(files: &[RustFile]) -> Vec<Finding> {
    let mut violations = BTreeSet::new();
    for file in files {
        let mut visitor = Visitor { path: &file.relative_path, violations: &mut violations };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

struct Visitor<'a> {
    path: &'a str,
    violations: &'a mut BTreeSet<Finding>,
}

impl Visitor<'_> {
    fn found(&mut self, line: usize, message: String) {
        self.violations.insert(Finding::new(self.path, line, message, REMEDY));
    }
}

/// Whether `ty` is `Result<_, String>`, as `Result`, `std::result::Result` or
/// `core::result::Result` spell it.
fn is_string_result(ty: &syn::Type) -> bool {
    let syn::Type::Path(path) = ty else {
        return false;
    };
    let Some(last) = path.path.segments.last() else {
        return false;
    };
    if last.ident != "Result" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &last.arguments else {
        return false;
    };
    let types: Vec<_> = arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            syn::GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
        .collect();
    matches!(types.as_slice(), [_, syn::Type::Path(error)] if error.qself.is_none()
        && error.path.is_ident("String"))
}

/// Whether a closure body makes text of its input: `to_string()` called on
/// anything, or `format!`.
fn makes_text(body: &syn::Expr) -> bool {
    match body {
        syn::Expr::MethodCall(call) => call.method == "to_string",
        syn::Expr::Macro(mac) => mac.mac.path.is_ident("format"),
        syn::Expr::Block(block) => match block.block.stmts.as_slice() {
            [syn::Stmt::Expr(expr, None)] => makes_text(expr),
            _ => false,
        },
        syn::Expr::Paren(paren) => makes_text(&paren.expr),
        _ => false,
    }
}

impl<'ast> Visit<'ast> for Visitor<'_> {
    fn visit_signature(&mut self, signature: &'ast syn::Signature) {
        if let syn::ReturnType::Type(_, ty) = &signature.output {
            if is_string_result(ty) {
                self.found(
                    signature.ident.span().start().line,
                    format!("{} returns Result<_, String>", signature.ident),
                );
            }
        }
        visit::visit_signature(self, signature);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "map_err" {
            if let [syn::Expr::Closure(closure)] = call.args.iter().collect::<Vec<_>>().as_slice() {
                if makes_text(&closure.body) {
                    self.found(
                        call.method.span().start().line,
                        "map_err turns an error into a string".to_string(),
                    );
                }
            }
        }
        visit::visit_expr_method_call(self, call);
    }
}

#[cfg(test)]
#[path = "typed_errors_tests.rs"]
mod tests;
