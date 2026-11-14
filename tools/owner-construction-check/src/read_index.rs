//! The workspace's functions, methods and struct fields, as the search for
//! read code (`read_code.rs`) looks callees and receivers up: each callable
//! with its generics' bounds, each struct's field types, and the names a type
//! mentions. Items gated on test support alone are left out.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit::{self, Visit};

use crate::module_sources::test_support_module_files;
use crate::name_resolution::{ItemPath, NameResolver};
use crate::policy::{Policy, ReadShapes};
use crate::syntax::{is_test_support_only, type_name, RustFile};

/// The names in a type's paths, bounds and arguments; the arguments a
/// callback type is given only when `callbacks`.
pub(crate) fn names(ty: &syn::Type, callbacks: bool) -> BTreeSet<String> {
    let mut found = Names { names: BTreeSet::new(), callbacks };
    found.visit_type(ty);
    found.names
}

fn bound_names<'b>(bounds: impl IntoIterator<Item = &'b syn::TypeParamBound>) -> BTreeSet<String> {
    let mut found = Names { names: BTreeSet::new(), callbacks: true };
    for bound in bounds {
        found.visit_type_param_bound(bound);
    }
    found.names
}

struct Names {
    names: BTreeSet<String>,
    callbacks: bool,
}

impl<'ast> Visit<'ast> for Names {
    fn visit_path_segment(&mut self, node: &'ast syn::PathSegment) {
        self.names.insert(node.ident.to_string());
        visit::visit_path_segment(self, node);
    }

    fn visit_parenthesized_generic_arguments(
        &mut self,
        node: &'ast syn::ParenthesizedGenericArguments,
    ) {
        if self.callbacks {
            visit::visit_parenthesized_generic_arguments(self, node);
        }
    }

    fn visit_type_fn_ptr(&mut self, node: &'ast syn::TypeFnPtr) {
        if self.callbacks {
            visit::visit_type_fn_ptr(self, node);
        }
    }
}

/// The type a type is, by name: references, `Result`, `Option` (their first
/// type argument), `Box`, `Rc`, `Arc` and the policy's shared pointers
/// peeled, and `Self` as `owner`. A collection is the type of what it holds
/// (a sequence or set its element, a map its values), since what is called on
/// its items is what the search follows.
pub(crate) fn peel(
    ty: &syn::Type,
    owner: Option<&str>,
    pointers: &BTreeSet<String>,
) -> Option<String> {
    match ty {
        syn::Type::Reference(reference) => peel(&reference.elem, owner, pointers),
        syn::Type::Paren(inner) => peel(&inner.elem, owner, pointers),
        syn::Type::Group(inner) => peel(&inner.elem, owner, pointers),
        syn::Type::Slice(inner) => peel(&inner.elem, owner, pointers),
        syn::Type::Array(inner) => peel(&inner.elem, owner, pointers),
        syn::Type::Path(path) if path.qself.is_none() => {
            let segment = path.path.segments.last()?;
            let name = segment.ident.to_string();
            if name == "Self" {
                return owner.map(str::to_string);
            }
            let wrapper = matches!(
                name.as_str(),
                "Result"
                    | "Option"
                    | "Box"
                    | "Rc"
                    | "Arc"
                    | "Vec"
                    | "VecDeque"
                    | "HashSet"
                    | "BTreeSet"
                    | "IndexSet"
                    | "HashMap"
                    | "BTreeMap"
                    | "IndexMap"
            ) || pointers.contains(&name);
            if wrapper {
                if let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments {
                    let mut types = arguments.args.iter().filter_map(|argument| match argument {
                        syn::GenericArgument::Type(inner) => Some(inner),
                        _ => None,
                    });
                    let held = if name.ends_with("Map") { types.next_back() } else { types.next() };
                    return peel(held?, owner, pointers);
                }
            }
            Some(name)
        }
        _ => None,
    }
}

/// Each generic and the names its bounds hold.
type Bounds = BTreeMap<String, BTreeSet<String>>;

/// A function or method of the workspace, as a read can call it.
pub(crate) struct Callable<'a> {
    pub(crate) file: &'a RustFile,
    /// The `impl` block's type, or the trait.
    pub(crate) owner: Option<String>,
    pub(crate) name: String,
    pub(crate) signature: &'a syn::Signature,
    pub(crate) body: Option<&'a syn::Block>,
    /// Each generic of the `impl`, trait and function and the names its
    /// bounds hold.
    pub(crate) bounds: Bounds,
    pub(crate) plumbing: bool,
}

impl Callable<'_> {
    pub(crate) fn item(&self) -> String {
        match &self.owner {
            Some(owner) => format!("{owner}::{}", self.name),
            None => self.name.clone(),
        }
    }

    /// The type of the `index`th argument's parameter. A path call
    /// `Type::method(receiver, ..)` counts the receiver as its first.
    pub(crate) fn parameter(&self, index: usize, path_call: bool) -> Option<&syn::Type> {
        let skipped = usize::from(path_call && self.signature.receiver().is_some());
        let index = index.checked_sub(skipped)?;
        self.signature
            .inputs
            .iter()
            .filter_map(|input| match input {
                syn::FnArg::Typed(typed) => Some(typed.ty.as_ref()),
                syn::FnArg::Receiver(_) => None,
            })
            .nth(index)
    }

    /// Every name the parameter's type holds, and its generics' bounds.
    fn parameter_names(&self, index: usize, path_call: bool) -> BTreeSet<String> {
        let mut found =
            self.parameter(index, path_call).map(|ty| names(ty, true)).unwrap_or_default();
        let bounded = found
            .iter()
            .filter_map(|name| self.bounds.get(name))
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        found.extend(bounded);
        found
    }

    pub(crate) fn takes_read_callback(
        &self,
        index: usize,
        path_call: bool,
        shapes: &ReadShapes,
    ) -> bool {
        !self.parameter_names(index, path_call).is_disjoint(&shapes.read_types)
    }

    pub(crate) fn takes_callback(&self, index: usize, path_call: bool) -> bool {
        let names = self.parameter_names(index, path_call);
        ["Fn", "FnMut", "FnOnce"].iter().any(|callback| names.contains(*callback))
    }
}

/// The workspace's functions, methods and struct fields.
pub(crate) struct Index<'a> {
    pub(crate) callables: Vec<Callable<'a>>,
    /// Each free function, by the path the resolver gives it.
    pub(crate) free: BTreeMap<ItemPath, usize>,
    /// Each method, by its type or trait and its name.
    pub(crate) methods: BTreeMap<(String, String), Vec<usize>>,
    /// Each method, by its name.
    pub(crate) by_name: BTreeMap<String, Vec<usize>>,
    /// Each named field's type, by its struct and its name.
    pub(crate) fields: BTreeMap<(String, String), String>,
    /// The structs, enums and `impl` types the workspace declares.
    pub(crate) declared: BTreeSet<String>,
    pub(crate) pointers: BTreeSet<String>,
}

impl<'a> Index<'a> {
    pub(crate) fn collect(
        files: &'a [RustFile],
        resolver: &NameResolver,
        policy: &Policy,
        shapes: &ReadShapes,
    ) -> Self {
        let mut index = Index {
            callables: Vec::new(),
            free: BTreeMap::new(),
            methods: BTreeMap::new(),
            by_name: BTreeMap::new(),
            fields: BTreeMap::new(),
            declared: BTreeSet::new(),
            pointers: policy.shared_pointer_types.clone(),
        };
        let features = policy.test_support_features();
        let support_modules = test_support_module_files(files, features);
        for file in files.iter().filter(|file| {
            !is_test_support_only(&file.syntax.attrs, features)
                && !support_modules.contains(&file.relative_path)
        }) {
            let mut collector = Collector {
                file,
                resolver,
                shapes,
                features,
                index: &mut index,
                owners: Vec::new(),
            };
            collector.visit_file(&file.syntax);
        }
        index
    }
}

struct Collector<'a, 'b> {
    file: &'a RustFile,
    resolver: &'b NameResolver,
    shapes: &'b ReadShapes,
    features: &'b [String],
    index: &'b mut Index<'a>,
    /// The enclosing `impl` blocks and traits: their type and generics.
    owners: Vec<Option<(String, Bounds)>>,
}

impl<'a> Collector<'a, '_> {
    fn gated(&self, attrs: &[syn::Attribute]) -> bool {
        is_test_support_only(attrs, self.features)
    }

    fn add(
        &mut self,
        owner: Option<String>,
        signature: &'a syn::Signature,
        body: Option<&'a syn::Block>,
        mut bounds: Bounds,
    ) -> usize {
        generic_bounds(&signature.generics, &mut bounds);
        let id = self.index.callables.len();
        let name = signature.ident.to_string();
        if let Some(owner) = &owner {
            self.index.methods.entry((owner.clone(), name.clone())).or_default().push(id);
            self.index.by_name.entry(name.clone()).or_default().push(id);
        }
        self.index.callables.push(Callable {
            file: self.file,
            owner,
            name,
            signature,
            body,
            bounds,
            plumbing: self.shapes.is_plumbing(&self.file.relative_path),
        });
        id
    }

    fn owner(&self) -> Option<(String, Bounds)> {
        self.owners.last().cloned().flatten()
    }
}

/// Each generic of `generics` and the names its inline and `where` bounds hold.
fn generic_bounds(generics: &syn::Generics, into: &mut Bounds) {
    for parameter in &generics.params {
        if let syn::GenericParam::Type(parameter) = parameter {
            into.entry(parameter.ident.to_string())
                .or_default()
                .extend(bound_names(&parameter.bounds));
        }
    }
    for predicate in generics.where_clause.iter().flat_map(|clause| &clause.predicates) {
        if let syn::WherePredicate::Type(predicate) = predicate {
            if let Some(name) = type_name(&predicate.bounded_ty) {
                into.entry(name).or_default().extend(bound_names(&predicate.bounds));
            }
        }
    }
}

impl<'a> Visit<'a> for Collector<'a, '_> {
    fn visit_item_mod(&mut self, node: &'a syn::ItemMod) {
        if !self.gated(&node.attrs) {
            visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'a syn::ItemFn) {
        if self.gated(&node.attrs) {
            return;
        }
        let path = self.resolver.definition(&self.file.relative_path, &node.sig.ident);
        let id = self.add(None, &node.sig, Some(&node.block), BTreeMap::new());
        self.index.free.insert(path, id);
        visit::visit_item_fn(self, node);
    }

    fn visit_item_struct(&mut self, node: &'a syn::ItemStruct) {
        if self.gated(&node.attrs) {
            return;
        }
        let name = node.ident.to_string();
        if let syn::Fields::Named(fields) = &node.fields {
            for field in &fields.named {
                let Some(field_name) = &field.ident else { continue };
                if let Some(ty) = peel(&field.ty, Some(&name), &self.index.pointers) {
                    self.index.fields.insert((name.clone(), field_name.to_string()), ty);
                }
            }
        }
        self.index.declared.insert(name);
        visit::visit_item_struct(self, node);
    }

    fn visit_item_enum(&mut self, node: &'a syn::ItemEnum) {
        if !self.gated(&node.attrs) {
            self.index.declared.insert(node.ident.to_string());
            visit::visit_item_enum(self, node);
        }
    }

    fn visit_item_impl(&mut self, node: &'a syn::ItemImpl) {
        if self.gated(&node.attrs) {
            return;
        }
        let owner = type_name(&node.self_ty).map(|owner| {
            let mut bounds = BTreeMap::new();
            generic_bounds(&node.generics, &mut bounds);
            (owner, bounds)
        });
        if let Some((owner, _)) = &owner {
            self.index.declared.insert(owner.clone());
        }
        self.owners.push(owner);
        visit::visit_item_impl(self, node);
        self.owners.pop();
    }

    fn visit_impl_item_fn(&mut self, node: &'a syn::ImplItemFn) {
        if self.gated(&node.attrs) {
            return;
        }
        if let Some((owner, bounds)) = self.owner() {
            self.add(Some(owner), &node.sig, Some(&node.block), bounds);
        }
        visit::visit_impl_item_fn(self, node);
    }

    fn visit_item_trait(&mut self, node: &'a syn::ItemTrait) {
        if self.gated(&node.attrs) {
            return;
        }
        let mut bounds = BTreeMap::new();
        generic_bounds(&node.generics, &mut bounds);
        self.owners.push(Some((node.ident.to_string(), bounds)));
        visit::visit_item_trait(self, node);
        self.owners.pop();
    }

    fn visit_trait_item_fn(&mut self, node: &'a syn::TraitItemFn) {
        if self.gated(&node.attrs) {
            return;
        }
        if let Some((owner, bounds)) = self.owner() {
            self.add(Some(owner), &node.sig, node.default.as_ref(), bounds);
        }
        visit::visit_trait_item_fn(self, node);
    }
}
