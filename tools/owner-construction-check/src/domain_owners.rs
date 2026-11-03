//! Each domain of the database names the owner that does its work
//! (Appendix F.2). A type holding that owner whose every method only passes
//! its call on to it is a second name for the owner: callers hold the owner,
//! or the owner does the work.
//!
//! A forwarding method's body is one call on a field of its type that holds
//! the owner, `self.field.method(…)`, passing the method's own parameters in
//! order, each by value or by reference, and nothing else; the call may be
//! awaited, followed by `?`, wrapped in `Ok`, or its error mapped with
//! `map_err`. The type's methods are its inherent methods taking `self`: a
//! trait's implementation answers to the trait, and a constructor builds the
//! type.

use std::collections::{BTreeMap, BTreeSet};

use crate::finding::Finding;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{type_name, RustFile};

pub(crate) fn find_domain_owner_forwarding(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    // Each owner and the domains it owns.
    let mut owners = BTreeMap::<&str, Vec<&str>>::new();
    for domain in policy.database.iter().flat_map(|database| &database.domains) {
        owners.entry(domain.owner.as_str()).or_default().push(domain.name.as_str());
    }
    // Each type's methods, with the owners each passes its call on to.
    let mut methods = BTreeMap::<String, Vec<BTreeSet<&str>>>::new();
    for file in files {
        for implementation in impls(&file.syntax.items) {
            let Some(holder) = type_name(&implementation.self_ty) else {
                continue;
            };
            let Some(declared) = graph.types.get(&holder) else {
                continue;
            };
            for method in &implementation.items {
                let syn::ImplItem::Fn(method) = method else {
                    continue;
                };
                if method.sig.receiver().is_none() {
                    continue;
                }
                let held = forwarded_field(method)
                    .and_then(|field| declared.named_fields.get(&field))
                    .map(|held| {
                        owners
                            .keys()
                            .copied()
                            .filter(|owner| *owner != holder && held.contains(*owner))
                            .collect()
                    })
                    .unwrap_or_default();
                methods.entry(holder.clone()).or_default().push(held);
            }
        }
    }
    let mut findings = Vec::new();
    for (holder, methods) in methods {
        // The owners every method passes its call on to.
        let forwarded = methods.iter().fold(None::<BTreeSet<&str>>, |common, owners| {
            Some(match common {
                None => owners.clone(),
                Some(common) => common.intersection(owners).copied().collect(),
            })
        });
        for owner in forwarded.into_iter().flatten() {
            let (path, line) = &graph.types[&holder].declarations[0];
            let domains = &owners[owner];
            findings.push(Finding::new(
                path,
                *line,
                format!(
                    "every method of {holder} only passes its call on to {owner}, which owns the {} domain{}",
                    domains.join(", "),
                    if domains.len() == 1 { "" } else { "s" },
                ),
                "a domain's owner does its work: callers hold the owner, or the owner does the work the wrapper adds",
            ));
        }
    }
    findings
}

/// Every inherent impl, inside inline modules too.
fn impls(items: &[syn::Item]) -> Vec<&syn::ItemImpl> {
    let mut found = Vec::new();
    for item in items {
        match item {
            syn::Item::Impl(implementation) if implementation.trait_.is_none() => {
                found.push(implementation);
            }
            syn::Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    found.extend(impls(items));
                }
            }
            _ => {}
        }
    }
    found
}

/// The field a method's whole body passes its call on to, when it does.
fn forwarded_field(method: &syn::ImplItemFn) -> Option<String> {
    let [syn::Stmt::Expr(body, None)] = method.block.stmts.as_slice() else {
        return None;
    };
    let call = forwarded_call(body)?;
    let syn::Expr::Field(receiver) = &*call.receiver else {
        return None;
    };
    let syn::Expr::Path(base) = &*receiver.base else {
        return None;
    };
    if !base.path.is_ident("self") {
        return None;
    }
    let syn::Member::Named(field) = &receiver.member else {
        return None;
    };
    let parameters = method
        .sig
        .inputs
        .iter()
        .filter_map(|input| match input {
            syn::FnArg::Typed(typed) => match &*typed.pat {
                syn::Pat::Ident(pattern) => Some(pattern.ident.to_string()),
                _ => None,
            },
            syn::FnArg::Receiver(_) => None,
        })
        .collect::<Vec<_>>();
    let arguments = call.args.iter().map(argument_name).collect::<Option<Vec<_>>>()?;
    (parameters.len() == method.sig.inputs.len() - 1 && arguments == parameters)
        .then(|| field.to_string())
}

/// The method call an expression is, under `.await`, `?`, `Ok(…)` and
/// `.map_err(…)`.
fn forwarded_call(expr: &syn::Expr) -> Option<&syn::ExprMethodCall> {
    match expr {
        syn::Expr::Await(awaited) => forwarded_call(&awaited.base),
        syn::Expr::Try(tried) => forwarded_call(&tried.expr),
        syn::Expr::Paren(inner) => forwarded_call(&inner.expr),
        syn::Expr::Call(call)
            if call.args.len() == 1
                && matches!(&*call.func, syn::Expr::Path(path) if path.path.is_ident("Ok")) =>
        {
            forwarded_call(&call.args[0])
        }
        syn::Expr::MethodCall(call) if call.method == "map_err" && call.args.len() == 1 => {
            forwarded_call(&call.receiver)
        }
        // A clone hands the owner out, which the owner rules read.
        syn::Expr::MethodCall(call) if call.method != "clone" => Some(call),
        _ => None,
    }
}

/// The parameter an argument passes on: its name, by value or by reference.
fn argument_name(argument: &syn::Expr) -> Option<String> {
    match argument {
        syn::Expr::Path(path) => path.path.get_ident().map(ToString::to_string),
        syn::Expr::Reference(reference) => argument_name(&reference.expr),
        _ => None,
    }
}

#[cfg(test)]
#[path = "domain_owners_tests.rs"]
mod tests;
