//! Memory kept between reads (the read rule): a read starts from the database
//! every time. Four forms, each a finding of the shape `kept-state`:
//!
//! - a type that holds a live query (a field whose type, at any depth through
//!   the workspace's types, is one of the policy's `live_query_types`) has
//!   another field, which is neither a live query nor a value stream
//!   (`value_stream_types`), whose type retains a `kept_state_types` type;
//! - such a type writes one of those other fields after construction: in a
//!   method that takes `self`, an assignment to `self.f`, through it or with
//!   a compound operator, or a call on `self.f` of a workspace method that
//!   takes `&mut self`;
//! - read code captures state (`kept_capture.rs`);
//! - a read-code method's `Self` type retains a kept-state type.
//!
//! The live-query and value-stream types themselves are not looked inside:
//! their locks are request plumbing.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::kept_capture::captured_state;
use crate::macros::parse_macro_body;
use crate::policy::{Policy, ReadShapes, Shape};
use crate::read_code::{Body, ReadCode};
use crate::read_index::Index;
use crate::row_work::Hit;
use crate::syntax::{collect_declared_types, RustFile, StructInfo};
use crate::type_identity::TypeIdentity;

/// The workspace's declared types and what they retain.
pub(crate) struct Graph<'a> {
    pub(crate) types: BTreeMap<String, StructInfo>,
    pub(crate) shapes: &'a ReadShapes,
}

impl Graph<'_> {
    fn is_request(&self, name: &str) -> bool {
        self.shapes.live_query_types.contains(name) || self.shapes.value_stream_types.contains(name)
    }

    /// The names and the types they retain, through the workspace's types;
    /// a live query or value stream is named but not looked inside.
    pub(crate) fn reach(&self, names: &BTreeSet<String>) -> BTreeSet<String> {
        let mut reached = BTreeSet::new();
        let mut pending = names.iter().cloned().collect::<Vec<_>>();
        while let Some(name) = pending.pop() {
            if !reached.insert(name.clone()) || self.is_request(&name) {
                continue;
            }
            if let Some(info) = self.types.get(&name) {
                pending.extend(info.field_types.iter().cloned());
            }
        }
        reached
    }

    /// The first kept-state type these names retain.
    pub(crate) fn kept(&self, names: &BTreeSet<String>) -> Option<String> {
        self.reach(names).into_iter().find(|name| self.shapes.kept_state_types.contains(name))
    }

    /// Whether these names retain a live query or value stream.
    fn retains_request(&self, names: &BTreeSet<String>) -> bool {
        self.reach(names).iter().any(|name| self.is_request(name))
    }

    fn retains_live_query(&self, names: &BTreeSet<String>) -> bool {
        self.reach(names).iter().any(|name| self.shapes.live_query_types.contains(name))
    }
}

/// A type that holds a live query: its fields beside it.
struct Holder<'a> {
    name: &'a str,
    info: &'a StructInfo,
    /// The fields that are no live query or value stream, with the names
    /// each holds.
    others: BTreeMap<&'a str, &'a BTreeSet<String>>,
}

pub(crate) fn kept_state(
    read: &ReadCode<'_>,
    files: &[RustFile],
    policy: &Policy,
    shapes: &ReadShapes,
) -> Vec<Hit> {
    let identity = TypeIdentity::of(files, &policy.phantom_type_arguments);
    let graph = Graph {
        types: collect_declared_types(files, &policy.shared_pointer_types, &identity),
        shapes,
    };
    let mut hits = Vec::new();
    let holders = holders(&graph);
    for holder in &holders {
        let Some((path, line)) =
            holder.info.declarations.iter().find(|(path, _)| !shapes.is_plumbing(path))
        else {
            continue;
        };
        let kept = holder
            .others
            .iter()
            .find_map(|(field, names)| graph.kept(names).map(|kept| (*field, kept)));
        if let Some((field, kept)) = kept {
            hits.push(Hit {
                path: path.clone(),
                line: *line,
                item: holder.name.to_string(),
                shape: Shape::KeptState,
                what: format!("keeps `{field}`, which holds `{kept}` state, beside its live query"),
            });
        }
    }
    writes_after_construction(read.index(), &holders, &mut hits);
    for unit in read.units() {
        let (Body::Function { .. }, Some(owner)) = (&unit.body, &unit.owner) else { continue };
        if let Some(kept) = graph.kept(&BTreeSet::from([owner.clone()])) {
            hits.push(Hit {
                path: unit.file.relative_path.clone(),
                line: unit.body_line(),
                item: unit.item.clone(),
                shape: Shape::KeptState,
                what: format!("reads through `{owner}`, which keeps `{kept}` state"),
            });
        }
    }
    hits.extend(captured_state(read, &graph, files));
    hits
}

/// The types that hold a live query, and are not request plumbing.
fn holders<'a>(graph: &'a Graph<'_>) -> Vec<Holder<'a>> {
    let mut holders = Vec::new();
    for (name, info) in &graph.types {
        if graph.is_request(name) || info.named_fields.is_empty() {
            continue;
        }
        if !info.named_fields.values().any(|names| graph.retains_live_query(names)) {
            continue;
        }
        let others = info
            .named_fields
            .iter()
            .filter(|(_, names)| !graph.retains_request(names))
            .map(|(field, names)| (field.as_str(), names))
            .collect();
        holders.push(Holder { name, info, others });
    }
    holders
}

/// The methods taking `self` of each holder that write a field beside its
/// live query.
fn writes_after_construction(index: &Index<'_>, holders: &[Holder<'_>], hits: &mut Vec<Hit>) {
    for callable in &index.callables {
        let (Some(owner), Some(body), Some(_)) =
            (&callable.owner, callable.body, callable.signature.receiver())
        else {
            continue;
        };
        let Some(holder) = holders.iter().find(|holder| holder.name == owner) else { continue };
        if callable.plumbing {
            continue;
        }
        let mut writes = SelfWrites { index, holder, owner, found: Vec::new() };
        writes.visit_block(body);
        hits.extend(writes.found.into_iter().map(|(line, field)| Hit {
            path: callable.file.relative_path.clone(),
            line,
            item: callable.item(),
            shape: Shape::KeptState,
            what: format!("writes `self.{field}` after construction"),
        }));
    }
}

struct SelfWrites<'a, 'b> {
    index: &'a Index<'b>,
    holder: &'a Holder<'a>,
    owner: &'a str,
    found: Vec<(usize, String)>,
}

/// The field of `self` an expression is, or is inside: `self.f`, `self.f.x`,
/// `*self.f`, `self.f[i]`.
pub(crate) fn self_field(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Field(field) => {
            let syn::Member::Named(name) = &field.member else { return None };
            match field.base.as_ref() {
                syn::Expr::Path(path) if path.path.is_ident("self") => Some(name.to_string()),
                base => self_field(base),
            }
        }
        syn::Expr::Unary(inner) => self_field(&inner.expr),
        syn::Expr::Index(inner) => self_field(&inner.expr),
        syn::Expr::Paren(inner) => self_field(&inner.expr),
        syn::Expr::Reference(inner) => self_field(&inner.expr),
        _ => None,
    }
}

/// Whether a workspace method of this type and name takes `&mut self`.
pub(crate) fn takes_mut_self(index: &Index<'_>, ty: &str, method: &str) -> bool {
    index.methods.get(&(ty.to_string(), method.to_string())).is_some_and(|ids| {
        ids.iter().any(|&id| {
            index.callables[id].signature.receiver().is_some_and(|receiver| {
                matches!(receiver.kind, syn::ReceiverKind::Reference(_, _, Some(_)))
            })
        })
    })
}

impl SelfWrites<'_, '_> {
    fn write_to(&mut self, target: &syn::Expr) {
        if let Some(field) = self_field(target) {
            if self.holder.others.contains_key(field.as_str()) {
                self.found.push((target.span().start().line, field));
            }
        }
    }
}

impl<'ast> Visit<'ast> for SelfWrites<'_, '_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_expr_assign(&mut self, node: &'ast syn::ExprAssign) {
        self.write_to(&node.left);
        visit::visit_expr_assign(self, node);
    }

    fn visit_expr_binary(&mut self, node: &'ast syn::ExprBinary) {
        if is_compound_assignment(&node.op) {
            self.write_to(&node.left);
        }
        visit::visit_expr_binary(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if let syn::Expr::Field(receiver) = node.receiver.as_ref() {
            let on_self = matches!(receiver.base.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self"));
            if let (true, syn::Member::Named(field)) = (on_self, &receiver.member) {
                let field_type =
                    self.index.fields.get(&(self.owner.to_string(), field.to_string()));
                if field_type
                    .is_some_and(|ty| takes_mut_self(self.index, ty, &node.method.to_string()))
                {
                    self.write_to(&node.receiver);
                }
            }
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

pub(crate) fn is_compound_assignment(op: &syn::BinOp) -> bool {
    use syn::BinOp::*;
    matches!(
        op,
        AddAssign(_)
            | SubAssign(_)
            | MulAssign(_)
            | DivAssign(_)
            | RemAssign(_)
            | BitXorAssign(_)
            | BitAndAssign(_)
            | BitOrAssign(_)
            | ShlAssign(_)
            | ShrAssign(_)
    )
}

#[cfg(test)]
#[path = "kept_state_tests.rs"]
mod tests;
