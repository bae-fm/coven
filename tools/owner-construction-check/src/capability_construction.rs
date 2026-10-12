//! Owners and capabilities are constructed at composition roots (§20.2). The policy names
//! interfaces; every implementation, including feature-gated fakes, contributes
//! a construction-only type. Explicit non-trait capabilities join the same set.
//!
//! Capability factory definitions may build their declared result; owner factories
//! require a root entry themselves. Both have their use sites checked.
//! Only the policy's named receiver factories may derive capabilities
//! from an injected owner. Test sources and `cfg(test)` items may assemble capabilities.
//! `Default` is forbidden on capability types: inferred `Default::default()`
//! cannot be resolved by a syntax checker.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::factories::Factories;
use crate::finding::Finding;
use crate::macros::{parse_macro_body, token_paths};
use crate::name_resolution::{qualified_path, NameResolver};
use crate::owner_graph::OwnerGraph;
use crate::policy::Policy;
use crate::syntax::{is_test_only, is_test_source, type_name, type_names, RustFile};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ConstructionKind {
    Value,
    DefaultImplementation,
}

/// Read implementations before inspecting use sites, across every source and
/// without evaluating cfg predicates. Both construction and parameter checks
/// use this set, so an implementation needs no additional policy entry.
pub(crate) fn construction_only_types(files: &[RustFile], policy: &Policy) -> BTreeSet<String> {
    let mut types = policy
        .construction_only_capability_types
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    for file in files {
        ImplementationCollector {
            traits: policy.capability_traits,
            types: &mut types,
        }
        .visit_file(&file.syntax);
    }
    types
}

struct ImplementationCollector<'a> {
    traits: &'a [&'a str],
    types: &'a mut BTreeSet<String>,
}

impl<'ast> Visit<'ast> for ImplementationCollector<'_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if let Some((path, _)) = &node.trait_ {
            if node.modifiers.polarity.is_none()
                && path.segments.last().is_some_and(|trait_name| {
                    self.traits.contains(&trait_name.ident.to_string().as_str())
                })
            {
                if let Some(name) = type_name(&node.self_ty) {
                    self.types.insert(name);
                }
            }
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

pub(crate) fn find_capability_construction_violations(
    files: &[RustFile],
    policy: &Policy,
    graph: &OwnerGraph,
) -> Vec<Finding> {
    let capabilities = construction_only_types(files, policy);
    let owners = graph
        .owners
        .iter()
        .filter(|name| {
            !policy.task_types.contains(&name.as_str())
                && !policy.capability_types.contains(&name.as_str())
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let constructed = capabilities.union(&owners).cloned().collect();
    let resolver = NameResolver::collect(files);
    let mut violations = BTreeSet::new();
    let factories = Factories::collect(files, &constructed, &resolver, &mut violations);
    for file in files {
        if is_test_source(&file.relative_path) || is_test_only(&file.syntax.attrs) {
            continue;
        }
        let mut visitor = ConstructionVisitor {
            path: &file.relative_path,
            policy,
            capabilities: &capabilities,
            owners: &owners,
            resolver: &resolver,
            factories: &factories,
            current_type: None,
            current_method: None,
            bindings: BTreeSet::new(),
            scope: ConstructionScope::Outside,
            violations: &mut violations,
        };
        visitor.visit_file(&file.syntax);
    }
    violations.into_iter().collect()
}

enum ConstructionScope {
    Outside,
    CompositionRoot,
    Factory(BTreeSet<String>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReferenceKind {
    Value,
    Call,
    Literal,
    Receiver,
}

struct TypeContext {
    name: String,
    path: syn::Path,
    implemented_trait: Option<syn::Path>,
}

impl TypeContext {
    fn new(path: syn::Path, implemented_trait: Option<syn::Path>) -> Self {
        Self {
            name: path.segments.last().expect("type name").ident.to_string(),
            path,
            implemented_trait,
        }
    }
}

struct ConstructionVisitor<'a> {
    path: &'a str,
    policy: &'a Policy,
    capabilities: &'a BTreeSet<String>,
    owners: &'a BTreeSet<String>,
    resolver: &'a NameResolver,
    factories: &'a Factories,
    current_type: Option<TypeContext>,
    current_method: Option<String>,
    bindings: BTreeSet<String>,
    scope: ConstructionScope,
    violations: &'a mut BTreeSet<Finding>,
}

impl ConstructionVisitor<'_> {
    fn check_expr_path(&mut self, node: &syn::ExprPath, kind: ReferenceKind) {
        let segments = match &node.qself {
            Some(qself) => match qself.ty.as_ref() {
                syn::Type::Path(ty) => Some(qualified_path(&ty.path)),
                _ => None,
            }
            .map(|owner| {
                owner
                    .into_iter()
                    .chain(
                        node.path
                            .segments
                            .iter()
                            .skip(qself.position)
                            .map(|segment| segment.ident.to_string()),
                    )
                    .collect::<Vec<_>>()
            }),
            None => Some(qualified_path(&node.path)),
        };
        if let Some(segments) = segments {
            let trait_path = node
                .qself
                .as_ref()
                .filter(|qself| qself.position > 0)
                .map(|qself| {
                    let mut path = qualified_path(&node.path);
                    path.truncate(qself.position + usize::from(node.path.leading_colon.is_some()));
                    path
                });
            self.check_value_path(&segments, node.span(), kind, trait_path.as_deref());
        }
    }

    fn check_callee(&mut self, node: &syn::Expr) {
        match node {
            syn::Expr::Path(path) => {
                self.check_expr_path(path, ReferenceKind::Call);
                visit::visit_expr_path(self, path);
            }
            syn::Expr::Paren(node) => self.check_callee(&node.expr),
            syn::Expr::Group(node) => self.check_callee(&node.expr),
            _ => self.visit_expr(node),
        }
    }

    fn resolve_self<'a>(&'a self, name: &'a str) -> &'a str {
        match (name, &self.current_type) {
            ("Self", Some(ty)) => &ty.name,
            _ => name,
        }
    }

    fn record(&mut self, name: &str, span: Span, kind: ConstructionKind) {
        let name = self.resolve_self(name);
        if !self.capabilities.contains(name) && !self.owners.contains(name) {
            return;
        }
        if kind == ConstructionKind::Value {
            match &self.scope {
                ConstructionScope::CompositionRoot => return,
                ConstructionScope::Factory(results)
                    if !self.owners.contains(name) && results.contains(name) =>
                {
                    return
                }
                _ => {}
            }
        }
        let subject = if self.owners.contains(name) {
            "owner"
        } else {
            "capability"
        };
        let message = match kind {
            ConstructionKind::Value => format!("{}::{} constructs {subject} {name} outside a composition root", self.current_type.as_ref().map(|ty| ty.name.as_str()).unwrap_or("<free>"), self.current_method.as_deref().unwrap_or("<item>")),
            ConstructionKind::DefaultImplementation => format!("implements Default for {subject} {name}, which permits implicit construction through Default::default()"),
        };
        self.violations.insert(Finding::new(self.path, span.start().line, message,
            "construct owners and capabilities explicitly at the listed roots; inject them elsewhere; do not implement or derive Default"));
    }

    fn check_value_path(
        &mut self,
        segments: &[String],
        span: Span,
        kind: ReferenceKind,
        trait_path: Option<&[String]>,
    ) {
        if kind != ReferenceKind::Literal
            && matches!(segments, [name] if self.bindings.contains(name))
        {
            return;
        }
        self.check_factory_path(segments, span, kind, trait_path);
    }

    fn check_factory_path(
        &mut self,
        segments: &[String],
        span: Span,
        kind: ReferenceKind,
        trait_path: Option<&[String]>,
    ) {
        let resolved = if segments.first().is_some_and(|name| name == "Self") {
            match &self.current_type {
                Some(ty) => self
                    .resolver
                    .resolve_type(self.path, ty.path.span(), &qualified_path(&ty.path))
                    .map(|mut path| {
                        path.extend_from_slice(&segments[1..]);
                        path
                    }),
                None => self.resolver.resolve(self.path, span, segments),
            }
        } else if kind == ReferenceKind::Literal {
            self.resolver.resolve_type(self.path, span, segments)
        } else {
            self.resolver.resolve(self.path, span, segments)
        };
        let resolved = resolved.and_then(|path| {
            let trait_path = trait_path
                .map(|name| self.resolver.resolve_type(self.path, span, name))
                .transpose()?;
            let implemented_trait = self
                .current_type
                .as_ref()
                .and_then(|context| context.implemented_trait.as_ref())
                .map(|path| {
                    self.resolver
                        .resolve_type(self.path, path.span(), &qualified_path(path))
                })
                .transpose()?;
            let products = self
                .factories
                .products(&path, trait_path.as_ref(), |path| {
                    implemented_trait.as_ref() == Some(path)
                        || self.resolver.path_in_scope(self.path, span, path)
                })?;
            Ok((path, products))
        });
        match resolved {
            Ok((path, products)) => {
                if kind != ReferenceKind::Receiver {
                    if self.resolver.types.contains(&path) {
                        self.record(
                            path.last().expect("type name"),
                            span,
                            ConstructionKind::Value,
                        );
                    }
                    if let [owner_path @ .., method] = path.as_slice() {
                        if matches!(method.as_str(), "new" | "default")
                            && self.resolver.types.contains(owner_path)
                        {
                            self.record(
                                owner_path.last().expect("constructor type"),
                                span,
                                ConstructionKind::Value,
                            );
                        }
                    }
                }
                if let Some(results) = products {
                    for result in results {
                        let permitted_receiver = path.len() >= 2
                            && self.policy.capability_factories.iter().any(
                                |(_, owner, method, product)| {
                                    *owner == path[path.len() - 2]
                                        && *method == path[path.len() - 1]
                                        && *product == result
                                },
                            );
                        if !permitted_receiver
                            && (kind != ReferenceKind::Receiver || self.owners.contains(result))
                        {
                            self.record(result, span, ConstructionKind::Value);
                        }
                    }
                }
            }
            Err(error) => {
                let relevant = segments
                    .last()
                    .is_some_and(|name| self.factories.free_names.contains(name))
                    || segments
                        .iter()
                        .any(|name| self.factories.associated_types.contains(name));
                let relevant = relevant
                    || segments
                        .iter()
                        .any(|name| self.capabilities.contains(name) || self.owners.contains(name));
                if relevant || matches!(kind, ReferenceKind::Call | ReferenceKind::Receiver) {
                    self.violations.insert(Finding::new(
                        self.path,
                        span.start().line,
                        format!(
                            "cannot resolve call {} in {}::{}: {error}",
                            segments.join("::"),
                            self.current_type
                                .as_ref()
                                .map(|ty| ty.name.as_str())
                                .unwrap_or("<free>"),
                            self.current_method.as_deref().unwrap_or("<item>")
                        ),
                        "use an unambiguous module path or import for calls to workspace factories",
                    ));
                }
            }
        }
    }

    fn check_own_method(&mut self, method: &str, span: Span) {
        if self.current_type.is_some() {
            self.check_factory_path(
                &["Self".into(), method.into()],
                span,
                ReferenceKind::Receiver,
                None,
            );
        }
    }

    fn check_item_type(&mut self, ty: &syn::Type, span: Span) {
        for name in type_names(ty) {
            self.record(&name, span, ConstructionKind::Value);
        }
    }

    fn factory_results(&self, signature: &syn::Signature) -> BTreeSet<String> {
        let syn::ReturnType::Type(_, output) = &signature.output else {
            return BTreeSet::new();
        };
        type_names(output)
            .iter()
            .map(|name| self.resolve_self(name).to_string())
            .filter(|name| self.capabilities.contains(name))
            .filter(|name| {
                signature.receiver().is_none()
                    || self.policy.capability_factories.iter().any(
                        |(path, owner, method, product)| {
                            *path == self.path
                                && self.current_type.as_ref().map(|ty| ty.name.as_str())
                                    == Some(*owner)
                                && signature.ident == *method
                                && *product == name
                        },
                    )
            })
            .collect()
    }

    fn check_default_derive(&mut self, name: &syn::Ident, attrs: &[syn::Attribute]) {
        for attribute in attrs {
            if derives_default(&attribute.meta) {
                self.record(
                    &name.to_string(),
                    attribute.span(),
                    ConstructionKind::DefaultImplementation,
                );
            }
        }
    }

    fn bind_pattern(&mut self, pattern: &syn::Pat) {
        struct Bindings<'a>(&'a mut BTreeSet<String>);
        impl<'ast> Visit<'ast> for Bindings<'_> {
            fn visit_pat_ident(&mut self, node: &'ast syn::PatIdent) {
                self.0.insert(node.ident.to_string());
                visit::visit_pat_ident(self, node);
            }
        }
        Bindings(&mut self.bindings).visit_pat(pattern);
    }

    fn bind_inputs(&mut self, signature: &syn::Signature) {
        for input in &signature.inputs {
            if let syn::FnArg::Typed(input) = input {
                self.bind_pattern(&input.pat);
            }
        }
    }
}

fn derives_default(meta: &syn::Meta) -> bool {
    let syn::Meta::List(list) = meta else {
        return false;
    };
    if !list.path.is_ident("derive") && !list.path.is_ident("cfg_attr") {
        return false;
    }
    let Ok(children) = list.parse_args_with(
        syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
    ) else {
        // Malformed derives/cfg_attr are compiler errors, not valid declarations.
        return false;
    };
    if list.path.is_ident("derive") {
        children.iter().any(|child| {
            child
                .path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "Default")
        })
    } else {
        children.iter().skip(1).any(derives_default)
    }
}

impl<'ast> Visit<'ast> for ConstructionVisitor<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        // Constants/statics declared inside a root are assembled there. Other
        // nested items are independent callables or scopes, not part of its work.
        let scope = if matches!(node, syn::Item::Const(_) | syn::Item::Static(_))
            && matches!(self.scope, ConstructionScope::CompositionRoot)
        {
            ConstructionScope::CompositionRoot
        } else {
            ConstructionScope::Outside
        };
        let previous = std::mem::replace(&mut self.scope, scope);
        let bindings = std::mem::take(&mut self.bindings);
        visit::visit_item(self, node);
        self.bindings = bindings;
        self.scope = previous;
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_test_only(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        if !is_test_only(&node.attrs) {
            let previous = self
                .current_type
                .replace(TypeContext::new(syn::Path::from(node.ident.clone()), None));
            visit::visit_item_trait(self, node);
            self.current_type = previous;
        }
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        if !is_test_only(&node.attrs) {
            let scope = ConstructionScope::Factory(self.factory_results(&node.sig));
            let previous = std::mem::replace(&mut self.scope, scope);
            let bindings = std::mem::take(&mut self.bindings);
            self.bind_inputs(&node.sig);
            visit::visit_trait_item_fn(self, node);
            self.bindings = bindings;
            self.scope = previous;
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if !is_test_only(&node.attrs) {
            let rooted =
                self.policy
                    .is_composition_root(self.path, "<free>", &node.sig.ident.to_string());
            let scope = if rooted {
                ConstructionScope::CompositionRoot
            } else {
                ConstructionScope::Factory(self.factory_results(&node.sig))
            };
            let previous = std::mem::replace(&mut self.scope, scope);
            let current_type = self.current_type.take();
            let method = self.current_method.replace(node.sig.ident.to_string());
            self.bind_inputs(&node.sig);
            visit::visit_item_fn(self, node);
            self.current_type = current_type;
            self.current_method = method;
            self.scope = previous;
        }
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_item_const(self, node);
        }
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_item_static(self, node);
        }
    }

    fn visit_impl_item_const(&mut self, node: &'ast syn::ImplItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_impl_item_const(self, node);
        }
    }

    fn visit_trait_item_const(&mut self, node: &'ast syn::TraitItemConst) {
        if !is_test_only(&node.attrs) {
            self.check_item_type(&node.ty, node.span());
            visit::visit_trait_item_const(self, node);
        }
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        if !is_test_only(&node.attrs) {
            self.check_default_derive(&node.ident, &node.attrs);
            visit::visit_item_struct(self, node);
        }
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        if !is_test_only(&node.attrs) {
            self.check_default_derive(&node.ident, &node.attrs);
            visit::visit_item_enum(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if is_test_only(&node.attrs) {
            return;
        }
        let previous = std::mem::replace(
            &mut self.current_type,
            match node.self_ty.as_ref() {
                syn::Type::Path(ty) => Some(TypeContext::new(
                    ty.path.clone(),
                    node.trait_.as_ref().map(|(path, _)| path.clone()),
                )),
                _ => None,
            },
        );
        if node.trait_.as_ref().is_some_and(|(path, _)| {
            path.segments
                .last()
                .is_some_and(|segment| segment.ident == "Default")
        }) {
            self.record("Self", node.span(), ConstructionKind::DefaultImplementation);
        }
        visit::visit_item_impl(self, node);
        self.current_type = previous;
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_test_only(&node.attrs) {
            return;
        }
        let method = node.sig.ident.to_string();
        let allowed = self.policy.is_composition_root(
            self.path,
            self.current_type
                .as_ref()
                .map(|ty| ty.name.as_str())
                .unwrap_or("<impl>"),
            &method,
        );
        let scope = if allowed {
            ConstructionScope::CompositionRoot
        } else {
            ConstructionScope::Factory(self.factory_results(&node.sig))
        };
        let previous = std::mem::replace(&mut self.scope, scope);
        let bindings = std::mem::take(&mut self.bindings);
        let method = self.current_method.replace(node.sig.ident.to_string());
        self.bind_inputs(&node.sig);
        visit::visit_impl_item_fn(self, node);
        self.current_method = method;
        self.bindings = bindings;
        self.scope = previous;
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        let bindings = self.bindings.clone();
        visit::visit_block(self, node);
        self.bindings = bindings;
    }

    fn visit_local(&mut self, node: &'ast syn::Local) {
        if is_test_only(&node.attrs) {
            return;
        }
        // The initializer (and let-else branch) sees the previous binding.
        visit::visit_local(self, node);
        self.bind_pattern(&node.pat);
    }

    fn visit_expr_closure(&mut self, node: &'ast syn::ExprClosure) {
        let bindings = self.bindings.clone();
        for input in &node.inputs {
            self.bind_pattern(input);
        }
        visit::visit_expr_closure(self, node);
        self.bindings = bindings;
    }

    fn visit_arm(&mut self, node: &'ast syn::Arm) {
        let bindings = self.bindings.clone();
        self.bind_pattern(&node.pat);
        visit::visit_arm(self, node);
        self.bindings = bindings;
    }

    fn visit_expr_for_loop(&mut self, node: &'ast syn::ExprForLoop) {
        self.visit_expr(&node.expr);
        let bindings = self.bindings.clone();
        self.bind_pattern(&node.pat);
        self.visit_block(&node.body);
        self.bindings = bindings;
    }

    fn visit_expr_let(&mut self, node: &'ast syn::ExprLet) {
        self.visit_expr(&node.expr);
        self.bind_pattern(&node.pat);
    }

    fn visit_expr_if(&mut self, node: &'ast syn::ExprIf) {
        let bindings = self.bindings.clone();
        self.visit_expr(&node.cond);
        self.visit_block(&node.then_branch);
        self.bindings = bindings;
        if let Some((_, branch)) = &node.else_branch {
            self.visit_expr(branch);
        }
    }

    fn visit_expr_while(&mut self, node: &'ast syn::ExprWhile) {
        let bindings = self.bindings.clone();
        self.visit_expr(&node.cond);
        self.visit_block(&node.body);
        self.bindings = bindings;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if is_test_only(&node.attrs) {
            return;
        }
        if matches!(node.receiver.as_ref(), syn::Expr::Path(path) if path.path.is_ident("self")) {
            self.check_own_method(&node.method.to_string(), node.span());
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_path(&mut self, node: &'ast syn::ExprPath) {
        self.check_expr_path(node, ReferenceKind::Value);
        visit::visit_expr_path(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if is_test_only(&node.attrs) {
            return;
        }
        self.check_callee(&node.func);
        for argument in &node.args {
            self.visit_expr(argument);
        }
    }

    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if is_test_only(&node.attrs) {
            return;
        }
        self.check_value_path(
            &qualified_path(&node.path),
            node.span(),
            ReferenceKind::Literal,
            None,
        );
        visit::visit_expr_struct(self, node);
    }

    fn visit_expr_block(&mut self, node: &'ast syn::ExprBlock) {
        if !is_test_only(&node.attrs) {
            visit::visit_expr_block(self, node);
        }
    }

    fn visit_stmt_macro(&mut self, node: &'ast syn::StmtMacro) {
        if !is_test_only(&node.attrs) {
            visit::visit_stmt_macro(self, node);
        }
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(expanded) = self.resolver.expanded_macro(self.path, node) {
            self.visit_file(expanded);
            return;
        }
        match parse_macro_body(node) {
            Some(body) => body.visit(self),
            None => {
                for path in token_paths(node.tokens.clone()) {
                    if path.after_dot
                        && path.receiver.as_deref() == Some("self")
                        && path.followed_by == Some(proc_macro2::Delimiter::Parenthesis)
                    {
                        self.check_own_method(&path.segments[0], path.span);
                    } else if !path.after_dot {
                        let mut segments = path.segments;
                        if path.leading_colon {
                            segments.insert(0, String::new());
                        }
                        self.check_value_path(&segments, path.span, ReferenceKind::Value, None);
                    }
                }
            }
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "capability_construction_tests.rs"]
mod tests;
