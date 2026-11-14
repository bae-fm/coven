//! Which values are one optional value: a binding declared `Option<..>`, an
//! initializer that yields one, a chain of `Option`'s own methods on one, or a
//! workspace function that returns one. A `filter` of such a value is no
//! iterator's row work (`row_work.rs`).

use super::Walker;

impl Walker<'_, '_> {
    /// Remembers a binding declared `Option<..>`.
    pub(super) fn note_option(&mut self, pattern: &syn::Pat, ty: &syn::Type) {
        if let (syn::Pat::Ident(ident), true) = (pattern, declares_option(ty)) {
            self.options.insert(ident.ident.to_string());
        }
    }

    /// Whether an expression is one optional value: a binding declared or
    /// initialized so, `Some(..)`, a method that yields one, a chain of
    /// `Option`'s own methods on one, or a workspace function that returns one
    /// (inside a `Result` when the expression is tried).
    pub(super) fn is_option(&self, expr: &syn::Expr) -> bool {
        const YIELDS: [&str; 8] =
            ["get", "first", "last", "next", "pop", "find", "find_map", "position"];
        const CHAINS: [&str; 14] = [
            "map", "and_then", "filter", "or", "or_else", "cloned", "copied", "as_ref", "as_deref",
            "as_mut", "take", "clone", "flatten", "inspect",
        ];
        match expr {
            syn::Expr::Paren(inner) => self.is_option(&inner.expr),
            syn::Expr::Group(inner) => self.is_option(&inner.expr),
            syn::Expr::Reference(inner) => self.is_option(&inner.expr),
            syn::Expr::Path(path) => {
                path.path.get_ident().is_some_and(|name| self.options.contains(&name.to_string()))
            }
            syn::Expr::Call(call) => match call.func.as_ref() {
                syn::Expr::Path(path) if path.path.is_ident("Some") => true,
                syn::Expr::Path(path) => {
                    self.path_targets(&path.path).iter().any(|&id| self.returns_option(id, false))
                }
                _ => false,
            },
            syn::Expr::MethodCall(call) => {
                let name = call.method.to_string();
                YIELDS.contains(&name.as_str())
                    || (CHAINS.contains(&name.as_str()) && self.is_option(&call.receiver))
            }
            syn::Expr::Try(inner) => match inner.expr.as_ref() {
                syn::Expr::Call(call) => match call.func.as_ref() {
                    syn::Expr::Path(path) => self
                        .path_targets(&path.path)
                        .iter()
                        .any(|&id| self.returns_option(id, true)),
                    _ => false,
                },
                _ => false,
            },
            _ => false,
        }
    }

    /// Whether a function returns an `Option`, or with `tried`, a `Result` of
    /// one.
    fn returns_option(&self, id: usize, tried: bool) -> bool {
        let syn::ReturnType::Type(_, ty) = &self.context.index.callables[id].signature.output
        else {
            return false;
        };
        if !tried {
            return declares_option(ty);
        }
        let syn::Type::Path(path) = ty.as_ref() else { return false };
        let Some(segment) = path.path.segments.last() else { return false };
        let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            return false;
        };
        segment.ident == "Result"
            && arguments.args.iter().find_map(|argument| match argument {
                syn::GenericArgument::Type(inner) => Some(declares_option(inner)),
                _ => None,
            }) == Some(true)
    }
}

/// Whether a type is written `Option<..>`, behind references.
pub(super) fn declares_option(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(inner) => declares_option(&inner.elem),
        syn::Type::Paren(inner) => declares_option(&inner.elem),
        syn::Type::Group(inner) => declares_option(&inner.elem),
        syn::Type::Path(path) => {
            path.path.segments.last().is_some_and(|last| last.ident == "Option")
        }
        _ => false,
    }
}
