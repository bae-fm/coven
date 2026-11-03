//! One operation owns its state (Appendix F.3): state a task puts behind a
//! shared pointer for its operation stays with tasks. A task's own field,
//! filled where the task is built with a new `Arc` (or another of the
//! policy's shared pointers), shares that value with whatever else points at
//! it; a type other than a task that holds the same type through a shared
//! pointer could keep it past the operation, as a directory clone kept a
//! bootstrap's lock for the life of the handle built from it. Only a handle
//! the task makes may hold it besides.
//!
//! The rule knows the value by its type, so it reads only a workspace type
//! that is state: an owner, or one holding shared or private state. A value
//! no one changes, such as a write's list of changes, may outlive the write;
//! and `Arc<Mutex<Vec<u8>>>` names no value of its own, so two fields of that
//! type need not share one. It reads no test code (`test_code.rs`).

use std::collections::{BTreeMap, BTreeSet};

use quote::ToTokens;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::finding::Finding;
use crate::macros::parse_macro_body;
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{type_name, RustFile};

const REMEDY: &str = "a task's state lives as long as its operation: keep it in the task, and give what outlives the task a plain value or a handle the task makes";

pub(crate) fn find_task_state_leaks(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let pointers = &policy.shared_pointer_types;
    let declared = declared_fields(files, pointers);
    let mut made = BTreeMap::<String, BTreeSet<String>>::new();
    for file in files {
        let mut visitor = MadeShared {
            policy,
            declared: &declared,
            current_type: None,
            locals: BTreeSet::new(),
            made: &mut made,
        };
        visitor.visit_file(&file.syntax);
    }
    let state = policy
        .shared_state_types
        .union(&policy.private_state_types)
        .cloned()
        .collect::<BTreeSet<_>>();
    made.retain(|shape, _| {
        graph.types.contains_key(shape)
            && (graph.owners.contains(shape) || !graph.retained(shape).is_disjoint(&state))
    });
    let mut findings = BTreeSet::new();
    for (holder, fields) in &declared {
        if policy.task_types.contains(holder) {
            continue;
        }
        for field in fields {
            for shape in &field.shared {
                let Some(tasks) = made.get(shape) else {
                    continue;
                };
                if policy.handles.get(holder).is_some_and(|maker| tasks.contains(maker)) {
                    continue;
                }
                for task in tasks {
                    findings.insert(Finding::new(
                        &field.path,
                        field.line,
                        format!(
                            "{holder} holds {shape}, which task {task} makes for its operation"
                        ),
                        REMEDY,
                    ));
                }
            }
        }
    }
    findings.into_iter().collect()
}

/// One field of a declared type: its name (or position), and the values it
/// holds through a shared pointer, each written as `shape` writes it.
struct DeclaredField {
    path: String,
    line: usize,
    name: String,
    shared: BTreeSet<String>,
}

/// Every struct's, enum's and alias's fields, by the type's name.
fn declared_fields(
    files: &[RustFile],
    pointers: &BTreeSet<String>,
) -> BTreeMap<String, Vec<DeclaredField>> {
    struct Collector<'a> {
        path: &'a str,
        pointers: &'a BTreeSet<String>,
        fields: BTreeMap<String, Vec<DeclaredField>>,
    }

    impl Collector<'_> {
        fn add(&mut self, owner: &syn::Ident, fields: &syn::Fields) {
            for (index, field) in fields.iter().enumerate() {
                let name =
                    field.ident.as_ref().map_or_else(|| index.to_string(), ToString::to_string);
                self.add_type(owner, name, &field.ty);
            }
        }

        fn add_type(&mut self, owner: &syn::Ident, name: String, ty: &syn::Type) {
            self.fields.entry(owner.to_string()).or_default().push(DeclaredField {
                path: self.path.to_string(),
                line: ty.span().start().line,
                name,
                shared: shared_shapes(ty, self.pointers),
            });
        }
    }

    impl<'ast> Visit<'ast> for Collector<'_> {
        fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
            self.add(&node.ident, &node.fields);
            visit::visit_item_struct(self, node);
        }

        fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
            for variant in &node.variants {
                self.add(&node.ident, &variant.fields);
            }
            visit::visit_item_enum(self, node);
        }

        fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
            self.add_type(&node.ident, String::new(), &node.ty);
            visit::visit_item_type(self, node);
        }

        fn visit_macro(&mut self, node: &'ast syn::Macro) {
            if let Some(body) = parse_macro_body(node) {
                body.visit(self);
            }
        }
    }

    let mut collector = Collector { path: "", pointers, fields: BTreeMap::new() };
    for file in files {
        collector.path = &file.relative_path;
        collector.visit_file(&file.syntax);
    }
    collector.fields
}

/// The values a type holds through one of `pointers`, each by its shape.
fn shared_shapes(ty: &syn::Type, pointers: &BTreeSet<String>) -> BTreeSet<String> {
    struct Pointed<'p> {
        pointers: &'p BTreeSet<String>,
        shapes: BTreeSet<String>,
    }

    impl<'ast> Visit<'ast> for Pointed<'_> {
        fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
            if let Some(segment) = node.path.segments.last() {
                if self.pointers.contains(&segment.ident.to_string()) {
                    if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                        for argument in &arguments.args {
                            if let syn::GenericArgument::Type(ty) = argument {
                                self.shapes.insert(shape(ty));
                            }
                        }
                    }
                }
            }
            visit::visit_type_path(self, node);
        }
    }

    let mut pointed = Pointed { pointers, shapes: BTreeSet::new() };
    pointed.visit_type(ty);
    pointed.shapes
}

/// A type as its names write it, without the paths that lead to them:
/// `std::sync::Mutex<Vec<u8>>` is `Mutex<Vec<u8>>`, as is `Mutex<Vec<u8>>`.
fn shape(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(path) => match path.path.segments.last() {
            Some(segment) => {
                let arguments = match &segment.arguments {
                    syn::PathArguments::AngleBracketed(arguments) => arguments
                        .args
                        .iter()
                        .map(|argument| match argument {
                            syn::GenericArgument::Type(ty) => shape(ty),
                            other => other.to_token_stream().to_string(),
                        })
                        .collect::<Vec<_>>(),
                    _ => Vec::new(),
                };
                if arguments.is_empty() {
                    segment.ident.to_string()
                } else {
                    format!("{}<{}>", segment.ident, arguments.join(", "))
                }
            }
            None => ty.to_token_stream().to_string(),
        },
        syn::Type::Reference(reference) => format!("&{}", shape(&reference.elem)),
        syn::Type::Tuple(tuple) => {
            format!("({})", tuple.elems.iter().map(shape).collect::<Vec<_>>().join(", "))
        }
        syn::Type::Paren(inner) => shape(&inner.elem),
        syn::Type::Group(inner) => shape(&inner.elem),
        other => other.to_token_stream().to_string(),
    }
}

/// Where a task is built with a new shared pointer in one of its fields:
/// the shapes of what those fields point at, with the task.
struct MadeShared<'a> {
    policy: &'a Policy,
    declared: &'a BTreeMap<String, Vec<DeclaredField>>,
    current_type: Option<String>,
    /// The function's local names bound to a new shared pointer.
    locals: BTreeSet<String>,
    made: &'a mut BTreeMap<String, BTreeSet<String>>,
}

impl MadeShared<'_> {
    fn task(&self, path: &syn::Path) -> Option<String> {
        let name = path.segments.last()?.ident.to_string();
        let name = match (name.as_str(), &self.current_type) {
            ("Self", Some(current)) => current.clone(),
            _ => name,
        };
        self.policy.task_types.contains(&name).then_some(name)
    }

    /// Whether `expr` is a new shared pointer: one made here, through `?`
    /// or inside `Some`, or a local bound to one.
    fn makes_shared(&self, expr: &syn::Expr) -> bool {
        match expr {
            syn::Expr::Paren(inner) => self.makes_shared(&inner.expr),
            syn::Expr::Group(inner) => self.makes_shared(&inner.expr),
            syn::Expr::Try(inner) => self.makes_shared(&inner.expr),
            syn::Expr::Path(path) => {
                path.path.get_ident().is_some_and(|name| self.locals.contains(&name.to_string()))
            }
            syn::Expr::Call(call) => match call.func.as_ref() {
                syn::Expr::Path(function) => {
                    let segments = &function.path.segments;
                    let last = segments.last().map(|segment| segment.ident.to_string());
                    let pointer = segments.len() >= 2
                        && self
                            .policy
                            .shared_pointer_types
                            .contains(&segments[segments.len() - 2].ident.to_string())
                        && matches!(last.as_deref(), Some("new" | "from" | "new_cyclic"));
                    pointer
                        || (matches!(last.as_deref(), Some("Some" | "Ok"))
                            && call.args.iter().any(|argument| self.makes_shared(argument)))
                }
                _ => false,
            },
            _ => false,
        }
    }

    fn record(&mut self, task: &str, field: &str) {
        let shapes = self
            .declared
            .get(task)
            .into_iter()
            .flatten()
            .filter(|declared| declared.name == field)
            .flat_map(|declared| declared.shared.iter().cloned())
            .collect::<Vec<_>>();
        for shape in shapes {
            self.made.entry(shape).or_default().insert(task.to_string());
        }
    }

    fn visit_body(&mut self, body: impl FnOnce(&mut Self)) {
        let locals = std::mem::take(&mut self.locals);
        body(self);
        self.locals = locals;
    }
}

impl<'ast> Visit<'ast> for MadeShared<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let previous = std::mem::replace(&mut self.current_type, type_name(&node.self_ty));
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let previous = self.current_type.take();
        self.visit_body(|visitor| visit::visit_item_fn(visitor, node));
        self.current_type = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        self.visit_body(|visitor| visit::visit_impl_item_fn(visitor, node));
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        visit::visit_local(self, node);
        if let (syn::Pat::Ident(name), Some(init)) = (&node.pat, &node.init) {
            if self.makes_shared(&init.expr) {
                self.locals.insert(name.ident.to_string());
            }
        }
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if let Some(task) = self.task(&node.path) {
            for field in &node.fields {
                if self.makes_shared(&field.expr) {
                    let name = match &field.member {
                        syn::Member::Named(name) => name.to_string(),
                        syn::Member::Unnamed(index) => index.index.to_string(),
                    };
                    self.record(&task, &name);
                }
            }
        }
        visit::visit_expr_struct(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(function) = node.func.as_ref() {
            if let Some(task) = self.task(&function.path) {
                for (index, argument) in node.args.iter().enumerate() {
                    if self.makes_shared(argument) {
                        self.record(&task, &index.to_string());
                    }
                }
            }
        }
        visit::visit_expr_call(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

#[cfg(test)]
#[path = "task_state_tests.rs"]
mod tests;
