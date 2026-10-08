//! Syntax helpers the checks share: the parsed source file, the type names a
//! type mentions, the declared types and their fields, and how a test source
//! or a test-only item is recognized.

use std::collections::{BTreeMap, BTreeSet};

use crate::macros::parse_macro_body;
use syn::visit::{self, Visit};

#[derive(Clone)]
pub(crate) struct RustFile {
    /// Relative to the workspace root, with `/` separators.
    pub(crate) relative_path: String,
    pub(crate) syntax: syn::File,
    /// How many lines the source holds.
    pub(crate) lines: usize,
}

impl RustFile {
    #[cfg(test)]
    pub(crate) fn fixture(relative_path: &str, source: &str) -> Self {
        RustFile {
            relative_path: relative_path.to_string(),
            syntax: syn::parse_file(source).expect("parse fixture"),
            lines: source.lines().count(),
        }
    }

    /// Whether the file belongs to one of the workspace's crates, which the
    /// §20.1 and §20.2 rules govern; tools answer only to the conventions.
    pub(crate) fn is_crate_source(&self) -> bool {
        self.relative_path.starts_with("crates/")
    }
}

/// The last segment of every type path and trait bound a type mentions.
#[derive(Default)]
pub(crate) struct TypeNames {
    pub(crate) names: BTreeSet<String>,
    callback_outputs_only: bool,
}

impl<'ast> Visit<'ast> for TypeNames {
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
        if let Some(segment) = node.path.segments.last() {
            self.names.insert(segment.ident.to_string());
        }
        visit::visit_type_path(self, node);
    }

    fn visit_type_trait_object(&mut self, node: &'ast syn::TypeTraitObject) {
        for bound in &node.bounds {
            if let syn::TypeParamBound::Trait(bound) = bound {
                if let Some(segment) = bound.path.segments.last() {
                    self.names.insert(segment.ident.to_string());
                }
            }
        }
        visit::visit_type_trait_object(self, node);
    }
}

pub(crate) fn type_names(ty: &syn::Type) -> BTreeSet<String> {
    let mut names = TypeNames::default();
    names.visit_type(ty);
    names.names
}

/// Types supplied to a callee. A callback receives its arguments from that
/// callee; only its return value can supply a capability to it.
pub(crate) fn supplied_type_names(ty: &syn::Type) -> BTreeSet<String> {
    let mut names = TypeNames {
        callback_outputs_only: true,
        ..TypeNames::default()
    };
    names.visit_type(ty);
    names.names
}

/// The type names a declared type's fields (or variants' fields, or alias
/// target) mention.
#[derive(Clone)]
pub(crate) struct StructInfo {
    pub(crate) field_types: BTreeSet<String>,
    pub(crate) declarations: Vec<(String, usize)>,
}

/// Structs, enums, aliases, traits and unions, with the type names their
/// fields mention.
pub(crate) fn collect_declared_types(files: &[RustFile]) -> BTreeMap<String, StructInfo> {
    let mut types = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| !is_test_source(&file.relative_path) && !is_test_only(&file.syntax.attrs))
    {
        DeclarationCollector {
            path: &file.relative_path,
            types: &mut types,
        }
        .visit_file(&file.syntax);
    }
    types
}

struct DeclarationCollector<'a> {
    path: &'a str,
    types: &'a mut BTreeMap<String, StructInfo>,
}

impl<'ast> Visit<'ast> for DeclarationCollector<'_> {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let (attrs, name) = match node {
            syn::Item::Struct(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Enum(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Union(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Trait(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Type(item) => (&item.attrs, Some(&item.ident)),
            syn::Item::Mod(item) => (&item.attrs, None),
            syn::Item::Fn(item) => (&item.attrs, None),
            syn::Item::Impl(item) => (&item.attrs, None),
            syn::Item::Const(item) => (&item.attrs, None),
            syn::Item::Static(item) => (&item.attrs, None),
            syn::Item::ForeignMod(item) => (&item.attrs, None),
            syn::Item::Macro(item) => (&item.attrs, None),
            _ => return,
        };
        if is_test_only(attrs) {
            return;
        }
        if let Some(name) = name {
            let fields = match node {
                syn::Item::Struct(item) => item
                    .fields
                    .iter()
                    .flat_map(|field| type_names(&field.ty))
                    .collect(),
                syn::Item::Enum(item) => item
                    .variants
                    .iter()
                    .flat_map(|variant| {
                        variant
                            .fields
                            .iter()
                            .flat_map(|field| type_names(&field.ty))
                    })
                    .collect(),
                syn::Item::Union(item) => item
                    .fields
                    .named
                    .iter()
                    .flat_map(|field| type_names(&field.ty))
                    .collect(),
                syn::Item::Type(item) => type_names(&item.ty),
                _ => BTreeSet::new(),
            };
            let info = self
                .types
                .entry(name.to_string())
                .or_insert_with(|| StructInfo {
                    field_types: BTreeSet::new(),
                    declarations: Vec::new(),
                });
            info.field_types.extend(fields);
            info.declarations
                .push((self.path.to_string(), name.span().start().line));
        }
        visit::visit_item(self, node);
    }

    fn visit_impl_item(&mut self, node: &'ast syn::ImplItem) {
        let attrs = match node {
            syn::ImplItem::Fn(item) => &item.attrs,
            syn::ImplItem::Const(item) => &item.attrs,
            syn::ImplItem::Type(item) => &item.attrs,
            syn::ImplItem::Macro(item) => &item.attrs,
            _ => return,
        };
        if !is_test_only(attrs) {
            visit::visit_impl_item(self, node);
        }
    }

    fn visit_trait_item(&mut self, node: &'ast syn::TraitItem) {
        let attrs = match node {
            syn::TraitItem::Fn(item) => &item.attrs,
            syn::TraitItem::Const(item) => &item.attrs,
            syn::TraitItem::Type(item) => &item.attrs,
            syn::TraitItem::Macro(item) => &item.attrs,
            _ => return,
        };
        if !is_test_only(attrs) {
            visit::visit_trait_item(self, node);
        }
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
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

pub(crate) fn output_contains_owner(output: &syn::ReturnType, owner: &str) -> bool {
    let syn::ReturnType::Type(_, output) = output else {
        return false;
    };
    let names = type_names(output);
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
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect()
}

/// A call path that could name a free function: one segment, or segments that
/// are all modules (`crate`, `self`, `super` or lowercase names).
pub(crate) fn could_be_free_function_path(segments: &[String]) -> bool {
    segments.len() == 1
        || segments[..segments.len() - 1].iter().all(|name| {
            matches!(name.as_str(), "crate" | "self" | "super")
                || name
                    .chars()
                    .next()
                    .is_some_and(|character| character.is_lowercase())
        })
}

/// A call path that could name an associated function of a local type:
/// `Type::function`, or one rooted at `crate`, `self` or `super`. An external
/// crate's `Store::new` does not match a local `Store`.
pub(crate) fn could_be_local_associated_function_path(segments: &[String]) -> bool {
    segments.len() == 2
        || (segments.len() > 2
            && segments
                .first()
                .is_some_and(|name| matches!(name.as_str(), "crate" | "self" | "super")))
}

/// The test layout of §20.3: a source's tests live beside it in
/// `<name>_tests.rs`, and integration tests in a crate's `tests/` directory.
/// Nothing else is test code — test-support modules behind a `test-utils`
/// feature are production sources and answer to every rule.
pub(crate) fn is_test_source(path: &str) -> bool {
    is_integration_test_source(path) || path.ends_with("_tests.rs")
}

/// A file under a crate's or tool's own `tests/` directory.
pub(crate) fn is_integration_test_source(path: &str) -> bool {
    let mut segments = path.split('/');
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (Some("crates" | "tools"), Some(_), Some("tests"))
    )
}

/// Whether an item compiles only into tests: a `#[test]` (or `#[tokio::test]`
/// and the like), or a `#[cfg(…)]` whose predicate holds only under `test`.
/// `cfg(not(test))`, `cfg(any(test, feature = "…"))` and
/// `cfg(feature = "test-utils")` all compile into production builds.
pub(crate) fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "test")
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

#[cfg(test)]
#[path = "syntax_tests.rs"]
mod tests;
