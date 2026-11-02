//! Syntax helpers the checks share: the parsed source file, the type names a
//! type mentions, the declared types and their fields, and how a test source
//! or a test-only item is recognized (`test_code.rs` removes both before the
//! rules of Appendix F.1 to F.3 read the crates).

use std::collections::{BTreeMap, BTreeSet};

use crate::macros::parse_macro_body;
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

/// The last segment of every type path and trait bound a type mentions. The
/// type arguments of a generic type in `phantoms` are left out: the type holds
/// no value of them (`Policy::phantom_type_arguments`).
pub(crate) struct TypeNames<'p> {
    pub(crate) names: BTreeSet<String>,
    callback_outputs_only: bool,
    phantoms: &'p BTreeSet<String>,
}

impl<'p> TypeNames<'p> {
    pub(crate) fn new(phantoms: &'p BTreeSet<String>) -> Self {
        Self { names: BTreeSet::new(), callback_outputs_only: false, phantoms }
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
        if let Some(segment) = node.path.segments.last() {
            let name = segment.ident.to_string();
            if self.phantoms.contains(&name) {
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
                if let Some(segment) = bound.path.segments.last() {
                    self.names.insert(segment.ident.to_string());
                }
            }
        }
        visit::visit_type_trait_object(self, node);
    }
}

pub(crate) fn type_names(ty: &syn::Type, phantoms: &BTreeSet<String>) -> BTreeSet<String> {
    let mut names = TypeNames::new(phantoms);
    names.visit_type(ty);
    names.names
}

/// Types supplied to a callee, or held by a field. A callback receives its
/// arguments from its caller; only its return value can supply a capability.
/// So a field holding a function pointer retains none of its argument types:
/// `fn(&AdminRequest)` holds no request.
pub(crate) fn supplied_type_names(ty: &syn::Type, phantoms: &BTreeSet<String>) -> BTreeSet<String> {
    let mut names = TypeNames { callback_outputs_only: true, ..TypeNames::new(phantoms) };
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
/// fields hold values of.
pub(crate) fn collect_declared_types(
    files: &[RustFile],
    phantoms: &BTreeSet<String>,
) -> BTreeMap<String, StructInfo> {
    let mut types = BTreeMap::new();
    for file in files {
        DeclarationCollector { path: &file.relative_path, phantoms, types: &mut types }
            .visit_file(&file.syntax);
    }
    types
}

struct DeclarationCollector<'a> {
    path: &'a str,
    phantoms: &'a BTreeSet<String>,
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
            let fields = match node {
                syn::Item::Struct(item) => item
                    .fields
                    .iter()
                    .flat_map(|field| supplied_type_names(&field.ty, self.phantoms))
                    .collect(),
                syn::Item::Enum(item) => item
                    .variants
                    .iter()
                    .flat_map(|variant| {
                        variant
                            .fields
                            .iter()
                            .flat_map(|field| supplied_type_names(&field.ty, self.phantoms))
                    })
                    .collect(),
                syn::Item::Union(item) => item
                    .fields
                    .named
                    .iter()
                    .flat_map(|field| supplied_type_names(&field.ty, self.phantoms))
                    .collect(),
                syn::Item::Type(item) => supplied_type_names(&item.ty, self.phantoms),
                _ => BTreeSet::new(),
            };
            let info = self.types.entry(name.to_string()).or_insert_with(|| StructInfo {
                field_types: BTreeSet::new(),
                declarations: Vec::new(),
            });
            info.field_types.extend(fields);
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
    phantoms: &BTreeSet<String>,
) -> bool {
    let syn::ReturnType::Type(_, output) = output else {
        return false;
    };
    let names = type_names(output, phantoms);
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

#[cfg(test)]
#[path = "syntax_tests.rs"]
mod tests;
