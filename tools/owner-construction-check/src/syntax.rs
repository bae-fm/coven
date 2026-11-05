//! Syntax helpers the checks share: the parsed source file, the type names a
//! type mentions, the declared types and their fields, and how a test source
//! or a test-only item is recognized (`test_code.rs` removes both before the
//! rules of Appendix F.1 to F.3 read the crates).

use std::collections::{BTreeMap, BTreeSet};

use crate::macros::parse_macro_body;
use crate::type_identity::{FileTypes, TypeIdentity};
use syn::visit::{self, Visit};

#[derive(Clone)]
pub(crate) struct RustFile {
    /// Relative to the workspace root, with `/` separators.
    pub(crate) relative_path: String,
    /// The directory of the workspace member that holds the file, relative to
    /// the workspace root and ending in `/`.
    pub(crate) package_root: String,
    /// That member's package name.
    pub(crate) package: String,
    pub(crate) syntax: syn::File,
    /// How many lines the source holds.
    pub(crate) lines: usize,
    /// Cargo dependency names, including aliases, mapped to their crate names.
    pub(crate) extern_crates: BTreeMap<String, String>,
    /// Whether only test-only module declarations reach the file, as one
    /// declared `#[cfg(test)] mod tests;` (`module_sources::test_module_files`).
    pub(crate) test_module: bool,
}

impl RustFile {
    /// A file of the member named by the path's first two segments, as in
    /// `crates/<package>/src/lib.rs`.
    #[cfg(test)]
    pub(crate) fn fixture(relative_path: &str, source: &str) -> Self {
        let mut segments = relative_path.split('/');
        let (Some(directory), Some(package)) = (segments.next(), segments.next()) else {
            panic!("fixture path {relative_path} names no member directory");
        };
        RustFile {
            relative_path: relative_path.to_string(),
            package_root: format!("{directory}/{package}/"),
            package: package.to_string(),
            syntax: syn::parse_file(source).expect("parse fixture"),
            lines: source.lines().count(),
            extern_crates: BTreeMap::new(),
            test_module: false,
        }
    }

    /// Test code: a file the test layout of Appendix F.5 names as tests, or
    /// one only test-only module declarations reach. Nothing else is —
    /// test-support modules behind a `test-utils` feature are production
    /// sources and answer to every rule.
    pub(crate) fn is_test_source(&self) -> bool {
        self.test_module || self.is_test_source_by_name()
    }

    /// The test layout of Appendix F.5: a source's tests live beside it in
    /// `<name>_tests.rs`, and integration tests in a member's `tests/`
    /// directory.
    pub(crate) fn is_test_source_by_name(&self) -> bool {
        self.is_integration_test_source() || self.relative_path.ends_with("_tests.rs")
    }

    /// A file under its member's own `tests/` directory.
    pub(crate) fn is_integration_test_source(&self) -> bool {
        self.relative_path
            .strip_prefix(self.package_root.as_str())
            .is_some_and(|path| path.starts_with("tests/"))
    }
}

/// The name of every type path and trait bound a type mentions, as `types`
/// knows it (`type_identity.rs`). The type arguments of a generic type in the
/// policy's phantoms are left out: the type holds no value of them
/// (`Policy::phantom_type_arguments`).
pub(crate) struct TypeNames<'p> {
    pub(crate) names: BTreeSet<String>,
    callback_outputs_only: bool,
    types: FileTypes<'p>,
}

impl<'p> TypeNames<'p> {
    pub(crate) fn new(types: FileTypes<'p>) -> Self {
        Self { names: BTreeSet::new(), callback_outputs_only: false, types }
    }
}

impl<'ast> Visit<'ast> for TypeNames<'_> {
    fn visit_parenthesized_generic_arguments(
        &mut self,
        node: &'ast syn::ParenthesizedGenericArguments,
    ) {
        if self.callback_outputs_only {
            self.visit_return_type(&node.output);
        } else {
            visit::visit_parenthesized_generic_arguments(self, node);
        }
    }

    fn visit_type_fn_ptr(&mut self, node: &'ast syn::TypeFnPtr) {
        if self.callback_outputs_only {
            self.visit_return_type(&node.output);
        } else {
            visit::visit_type_fn_ptr(self, node);
        }
    }

    fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
        if let Some(projected) = self.types.projection(node) {
            self.names.extend(projected.iter().cloned());
            return;
        }
        if let Some(name) = self.types.name(&node.path) {
            if self.types.phantoms.contains(&name) {
                if let Some(qself) = &node.qself {
                    self.visit_qself(qself);
                }
                for outer in node.path.segments.iter().take(node.path.segments.len() - 1) {
                    self.visit_path_segment(outer);
                }
                self.names.insert(name);
                return;
            }
            self.names.insert(name);
        }
        visit::visit_type_path(self, node);
    }

    fn visit_type_trait_object(&mut self, node: &'ast syn::TypeTraitObject) {
        for bound in &node.bounds {
            if let syn::TypeParamBound::Trait(bound) = bound {
                self.names.extend(self.types.name(&bound.path));
            }
        }
        visit::visit_type_trait_object(self, node);
    }
}

pub(crate) fn type_names(ty: &syn::Type, types: FileTypes<'_>) -> BTreeSet<String> {
    let mut names = TypeNames::new(types);
    names.visit_type(ty);
    names.names
}

/// Types supplied to a callee, or held by a field. A callback receives its
/// arguments from its caller; only its return value can supply a capability.
/// So a field holding a function pointer retains none of its argument types:
/// `fn(&AdminRequest)` holds no request.
pub(crate) fn supplied_type_names(ty: &syn::Type, types: FileTypes<'_>) -> BTreeSet<String> {
    let mut names = TypeNames { callback_outputs_only: true, ..TypeNames::new(types) };
    names.visit_type(ty);
    names.names
}

/// One argument a callback can keep of what its callable gives it
/// (`kept_callback_arguments`).
pub(crate) struct KeptArgument<'ast> {
    /// The parameter holding the callback, when one parameter does: its
    /// type is the callback's, or a generic the callback bounds.
    pub(crate) parameter: Option<String>,
    /// The argument's place among the callback's.
    pub(crate) index: usize,
    pub(crate) ty: &'ast syn::Type,
}

/// What a signature's callbacks can keep of what the callable gives them:
/// each argument type of a callback its parameters' types or its generics'
/// bounds name, `impl FnOnce(&'a Connection)` or `fn(Permit)`, except one
/// lent only for the callback's call. A reference, or a value borrowing,
/// whose every lifetime is elided, `'_`, or bound by the callback's own
/// `for<…>` is lent so: `impl FnOnce(&Connection)` lets the callback use
/// the connection while it runs and keep none of it. An argument the
/// callback can keep is handed out, as a returned value is.
pub(crate) fn kept_callback_arguments(signature: &syn::Signature) -> Vec<KeptArgument<'_>> {
    let mut arguments =
        KeptCallbackArguments { bound: Vec::new(), origin: Origin::Unknown, kept: Vec::new() };
    let mut typed = Vec::new();
    for input in &signature.inputs {
        if let syn::FnArg::Typed(input) = input {
            let name = match input.pat.as_ref() {
                syn::Pat::Ident(name) => Some(name.ident.to_string()),
                _ => None,
            };
            arguments.origin = name.clone().map_or(Origin::Unknown, Origin::Parameter);
            arguments.visit_type(&input.ty);
            typed.push((name, input.ty.as_ref()));
        }
    }
    for parameter in &signature.generics.params {
        if let syn::GenericParam::Type(parameter) = parameter {
            arguments.origin = Origin::Generic(parameter.ident.to_string());
            for bound in &parameter.bounds {
                arguments.visit_type_param_bound(bound);
            }
        }
    }
    for predicate in signature.generics.where_clause.iter().flat_map(|clause| &clause.predicates) {
        if let syn::WherePredicate::Type(predicate) = predicate {
            arguments.origin = generic_name(&predicate.bounded_ty)
                .map_or(Origin::Unknown, |name| Origin::Generic(name.to_string()));
            arguments.visit_predicate_type(predicate);
        }
    }
    arguments
        .kept
        .into_iter()
        .map(|(origin, index, ty)| {
            let parameter = match origin {
                Origin::Parameter(name) => Some(name),
                Origin::Generic(generic) => {
                    let mut holders = typed
                        .iter()
                        .filter(|(_, ty)| generic_name(ty).is_some_and(|name| *name == generic));
                    match (holders.next(), holders.next()) {
                        (Some((name, _)), None) => name.clone(),
                        _ => None,
                    }
                }
                Origin::Unknown => None,
            };
            KeptArgument { parameter, index, ty }
        })
        .collect()
}

/// The generic a type names, by itself or behind a reference: `F`, `&F` or
/// `&mut F`.
fn generic_name(ty: &syn::Type) -> Option<&syn::Ident> {
    match ty {
        syn::Type::Reference(reference) => generic_name(&reference.elem),
        syn::Type::Paren(inner) => generic_name(&inner.elem),
        syn::Type::Path(path) if path.qself.is_none() => path.path.get_ident(),
        _ => None,
    }
}

/// Where a callback's type is written: a parameter's type, or a generic's
/// bounds.
#[derive(Clone)]
enum Origin {
    Parameter(String),
    Generic(String),
    Unknown,
}

struct KeptCallbackArguments<'ast> {
    /// The lifetimes the enclosing `for<…>` binders bind.
    bound: Vec<&'ast syn::Lifetime>,
    origin: Origin,
    kept: Vec<(Origin, usize, &'ast syn::Type)>,
}

impl<'ast> KeptCallbackArguments<'ast> {
    fn bind<T: ?Sized>(
        &mut self,
        binder: Option<&'ast syn::BoundLifetimes>,
        node: &'ast T,
        visit: impl FnOnce(&mut Self, &'ast T),
    ) {
        let depth = self.bound.len();
        for parameter in binder.into_iter().flat_map(|binder| &binder.lifetimes) {
            if let syn::GenericParam::Lifetime(parameter) = parameter {
                self.bound.push(&parameter.lifetime);
            }
        }
        visit(self, node);
        self.bound.truncate(depth);
    }

    /// Each callback argument the callback can keep. What a callback's own
    /// arguments and result carry flows between the callback and its
    /// caller, not out of the callable.
    fn arguments(&mut self, arguments: impl IntoIterator<Item = &'ast syn::Type>) {
        for (index, argument) in arguments.into_iter().enumerate() {
            if !self.lent(argument) {
                self.kept.push((self.origin.clone(), index, argument));
            }
        }
    }

    fn lent(&self, ty: &syn::Type) -> bool {
        let local = |lifetime: &syn::Lifetime| {
            lifetime.ident == "_" || self.bound.iter().any(|bound| bound.ident == lifetime.ident)
        };
        match ty {
            syn::Type::Paren(inner) => self.lent(&inner.elem),
            syn::Type::Group(inner) => self.lent(&inner.elem),
            syn::Type::Reference(reference) => reference.lifetime.as_ref().is_none_or(local),
            syn::Type::Path(path) => {
                let lifetimes = path
                    .path
                    .segments
                    .iter()
                    .flat_map(|segment| match &segment.arguments {
                        syn::PathArguments::AngleBracketed(arguments) => {
                            arguments.args.iter().collect::<Vec<_>>()
                        }
                        _ => Vec::new(),
                    })
                    .filter_map(|argument| match argument {
                        syn::GenericArgument::Lifetime(lifetime) => Some(lifetime),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                !lifetimes.is_empty() && lifetimes.into_iter().all(local)
            }
            _ => false,
        }
    }
}

impl<'ast> Visit<'ast> for KeptCallbackArguments<'ast> {
    fn visit_trait_bound(&mut self, node: &'ast syn::TraitBound) {
        self.bind(node.lifetimes.as_ref(), node, |this, node| {
            visit::visit_trait_bound(this, node);
        });
    }

    fn visit_predicate_type(&mut self, node: &'ast syn::PredicateType) {
        self.bind(node.lifetimes.as_ref(), node, |this, node| {
            visit::visit_predicate_type(this, node);
        });
    }

    fn visit_parenthesized_generic_arguments(
        &mut self,
        node: &'ast syn::ParenthesizedGenericArguments,
    ) {
        self.arguments(node.inputs.iter().map(|input| &input.ty));
    }

    fn visit_type_fn_ptr(&mut self, node: &'ast syn::TypeFnPtr) {
        self.bind(node.lifetimes.as_ref(), node, |this, node| {
            this.arguments(node.inputs.iter().map(|input| &input.ty));
        });
    }
}

/// The types a type holds through a shared pointer, such as `Arc`, or a
/// reference: values its holder shares with whatever else points at them.
/// `Arc<Mutex<State>>` holds `Mutex` and `State` so; `Mutex<State>` holds
/// neither. Names reach the result as `supplied_type_names` finds them.
pub(crate) fn shared_type_names(
    ty: &syn::Type,
    pointers: &BTreeSet<String>,
    types: FileTypes<'_>,
) -> BTreeSet<String> {
    let mut names = SharedTypeNames { pointers, types, names: BTreeSet::new() };
    names.visit_type(ty);
    names.names
}

struct SharedTypeNames<'p> {
    pointers: &'p BTreeSet<String>,
    types: FileTypes<'p>,
    names: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for SharedTypeNames<'_> {
    fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
        if self.types.projection(node).is_some() {
            return;
        }
        let (Some(segment), Some(name)) = (node.path.segments.last(), self.types.name(&node.path))
        else {
            return;
        };
        if self.pointers.contains(&name) {
            if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                for argument in &arguments.args {
                    if let syn::GenericArgument::Type(ty) = argument {
                        self.names.extend(supplied_type_names(ty, self.types));
                    }
                }
            }
        } else if !self.types.phantoms.contains(&name) {
            visit::visit_type_path(self, node);
        }
    }

    fn visit_type_reference(&mut self, node: &'ast syn::TypeReference) {
        self.names.extend(supplied_type_names(&node.elem, self.types));
    }

    // A callback holds none of its arguments (`supplied_type_names`).
    fn visit_parenthesized_generic_arguments(
        &mut self,
        node: &'ast syn::ParenthesizedGenericArguments,
    ) {
        self.visit_return_type(&node.output);
    }

    fn visit_type_fn_ptr(&mut self, node: &'ast syn::TypeFnPtr) {
        self.visit_return_type(&node.output);
    }
}

/// The type names a declared type's fields (or variants' fields, or alias
/// target) mention, and those among them it holds through a shared pointer
/// or a reference (`shared_type_names`).
#[derive(Clone)]
pub(crate) struct StructInfo {
    pub(crate) field_types: BTreeSet<String>,
    pub(crate) shared_field_types: BTreeSet<String>,
    /// A struct's named fields, each with the type names it holds values of.
    pub(crate) named_fields: BTreeMap<String, BTreeSet<String>>,
    /// Where each declaration is, by file and line.
    pub(crate) declarations: Vec<(String, usize)>,
}

/// Structs, enums, aliases, traits and unions, with the type names their
/// fields hold values of, and those they hold through one of `pointers` or
/// a reference, as `identity` names them.
pub(crate) fn collect_declared_types(
    files: &[RustFile],
    pointers: &BTreeSet<String>,
    identity: &TypeIdentity,
) -> BTreeMap<String, StructInfo> {
    let mut types = BTreeMap::new();
    for file in files {
        DeclarationCollector {
            path: &file.relative_path,
            pointers,
            names: identity.in_file(&file.relative_path),
            types: &mut types,
        }
        .visit_file(&file.syntax);
    }
    types
}

struct DeclarationCollector<'a> {
    path: &'a str,
    pointers: &'a BTreeSet<String>,
    names: FileTypes<'a>,
    types: &'a mut BTreeMap<String, StructInfo>,
}

impl<'ast> Visit<'ast> for DeclarationCollector<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let name = match node {
            syn::Item::Struct(item) => Some(&item.ident),
            syn::Item::Enum(item) => Some(&item.ident),
            syn::Item::Union(item) => Some(&item.ident),
            syn::Item::Trait(item) => Some(&item.ident),
            syn::Item::Type(item) => Some(&item.ident),
            _ => None,
        };
        if let Some(name) = name {
            let fields: Vec<&syn::Type> = match node {
                syn::Item::Struct(item) => item.fields.iter().map(|field| &field.ty).collect(),
                syn::Item::Enum(item) => item
                    .variants
                    .iter()
                    .flat_map(|variant| variant.fields.iter().map(|field| &field.ty))
                    .collect(),
                syn::Item::Union(item) => item.fields.named.iter().map(|field| &field.ty).collect(),
                syn::Item::Type(item) => vec![item.ty.as_ref()],
                _ => Vec::new(),
            };
            let info = self.types.entry(name.to_string()).or_insert_with(|| StructInfo {
                field_types: BTreeSet::new(),
                shared_field_types: BTreeSet::new(),
                named_fields: BTreeMap::new(),
                declarations: Vec::new(),
            });
            for ty in fields {
                info.field_types.extend(supplied_type_names(ty, self.names));
                info.shared_field_types.extend(shared_type_names(ty, self.pointers, self.names));
            }
            if let syn::Item::Struct(item) = node {
                for field in &item.fields {
                    if let Some(field_name) = &field.ident {
                        info.named_fields
                            .entry(field_name.to_string())
                            .or_default()
                            .extend(supplied_type_names(&field.ty, self.names));
                    }
                }
            }
            info.declarations.push((self.path.to_string(), name.span().start().line));
        }
        visit::visit_item(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
    }
}

pub(crate) fn type_name(ty: &syn::Type) -> Option<String> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    path.path.segments.last().map(|segment| segment.ident.to_string())
}

pub(crate) fn output_contains_owner(
    output: &syn::ReturnType,
    owner: &str,
    types: FileTypes<'_>,
) -> bool {
    let syn::ReturnType::Type(_, output) = output else {
        return false;
    };
    let names = type_names(output, types);
    names.contains("Self") || names.contains(owner)
}

/// Whether an item with this visibility is reachable outside its own module.
pub(crate) fn visibility_crosses_owner(visibility: &syn::Visibility) -> bool {
    match visibility {
        syn::Visibility::Inherited => false,
        syn::Visibility::Restricted(restricted) => !restricted.path.is_ident("self"),
        syn::Visibility::Public(_) => true,
    }
}

/// The identifiers of a path's segments.
pub(crate) fn path_names(path: &syn::Path) -> Vec<String> {
    path.segments.iter().map(|segment| segment.ident.to_string()).collect()
}

/// Whether an item compiles only into tests: a `#[test]` (or `#[tokio::test]`
/// and the like), or a `#[cfg(…)]` whose predicate holds only under `test`.
/// `cfg(not(test))`, `cfg(any(test, feature = "…"))` and
/// `cfg(feature = "test-utils")` all compile into production builds.
pub(crate) fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        attribute.path().segments.last().is_some_and(|segment| segment.ident == "test")
            || (attribute.path().is_ident("cfg")
                && attribute
                    .parse_args::<syn::Meta>()
                    .is_ok_and(|predicate| cfg_requires_test(&predicate)))
    })
}

/// Whether a `cfg` predicate can hold only when `test` does.
fn cfg_requires_test(predicate: &syn::Meta) -> bool {
    match predicate {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(list) => {
            let Ok(children) = list.parse_args_with(
                syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
            ) else {
                return false;
            };
            if list.path.is_ident("all") {
                children.iter().any(cfg_requires_test)
            } else if list.path.is_ident("any") {
                !children.is_empty() && children.iter().all(cfg_requires_test)
            } else {
                false
            }
        }
        syn::Meta::NameValue(_) => false,
    }
}

/// Every path a `use` tree imports, one segment list per leaf. A glob yields
/// its prefix.
pub(crate) fn flatten_use_tree(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    output: &mut Vec<Vec<String>>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, output);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let mut segments = prefix.clone();
            segments.push(name.ident.to_string());
            output.push(segments);
        }
        syn::UseTree::Rename(rename) => {
            let mut segments = prefix.clone();
            segments.push(rename.ident.to_string());
            output.push(segments);
        }
        syn::UseTree::Glob(_) => {
            output.push(prefix.clone());
        }
        syn::UseTree::Group(group) => {
            for item in &group.items {
                flatten_use_tree(item, prefix, output);
            }
        }
    }
}

/// Whether `node` is an `impl Clone` whose `clone` is `Self { .. }` with each
/// field set to `self.<that field>.clone()`, as `derive(Clone)` writes it
/// for a struct with named fields. Written out where the derive would bound
/// a type parameter, or one of its associated types, by `Clone`, it makes no
/// new value of anything the struct holds.
pub(crate) fn clones_each_field(node: &syn::ItemImpl) -> bool {
    let is_clone = node
        .trait_
        .as_ref()
        .is_some_and(|(path, _)| path.segments.last().is_some_and(|last| last.ident == "Clone"));
    let [syn::ImplItem::Fn(function)] = node.items.as_slice() else {
        return false;
    };
    let [syn::Stmt::Expr(syn::Expr::Struct(value), None)] = function.block.stmts.as_slice() else {
        return false;
    };
    is_clone
        && function.sig.ident == "clone"
        && value.path.is_ident("Self")
        && value.rest.is_none()
        && value.fields.iter().all(|field| {
            let syn::Member::Named(name) = &field.member else {
                return false;
            };
            let syn::Expr::MethodCall(call) = &field.expr else {
                return false;
            };
            let syn::Expr::Field(source) = call.receiver.as_ref() else {
                return false;
            };
            call.method == "clone"
                && call.args.is_empty()
                && matches!(&source.member, syn::Member::Named(own) if own == name)
                && matches!(source.base.as_ref(), syn::Expr::Path(base) if base.path.is_ident("self"))
        })
}

#[cfg(test)]
#[path = "syntax_tests.rs"]
mod tests;
