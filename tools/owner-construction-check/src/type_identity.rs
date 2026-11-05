//! Which type a type path names, as the rules match types: by name. Each type
//! the workspace declares is known by its name, which the rules that reason
//! about it hold to one declaration (`unique_type_names.rs`), and so is each
//! type of a dependency or the standard library, which the policy names in
//! `external_types`. A path that names an external type sharing its name with
//! a workspace type, as `rusqlite::Connection` beside a workspace
//! `Connection`, names neither of them for the rules: it is known by the path
//! it resolves to, which no rule and no policy row names (`name_resolution.rs`).
//!
//! A path that does not resolve, such as a generic parameter, keeps its name.
//! A projection of an associated type, as `M::Open` or `<M as Mode>::Open`,
//! names every type the workspace's trait impls give that associated type:
//! a type holding `M::Open` may hold any of them.

use std::collections::{BTreeMap, BTreeSet};

use syn::spanned::Spanned;
use syn::visit::{self, Visit};

use crate::macros::parse_macro_body;
use crate::name_resolution::{qualified_path, ItemPath, NameResolver};
use crate::syntax::RustFile;

/// Where a path's last segment starts, and the path as written: the paths
/// a macro expansion writes share the invocation's position.
type PathKey = (usize, usize, ItemPath);

pub(crate) struct TypeIdentity {
    /// The policy's generic types whose type arguments are no value they hold
    /// (`Policy::phantom_type_arguments`).
    phantoms: BTreeSet<String>,
    /// Per file, each path naming an external type under a workspace type's
    /// name, and the path it resolves to.
    external: BTreeMap<String, BTreeMap<PathKey, String>>,
    /// Each associated type a trait impl gives a type, by its name, and the
    /// names of the types the impls give it.
    projections: BTreeMap<String, BTreeSet<String>>,
}

impl TypeIdentity {
    pub(crate) fn of(files: &[RustFile], phantoms: &BTreeSet<String>) -> Self {
        let resolver = NameResolver::collect(files);
        let declared =
            resolver.types.iter().filter_map(|path| path.last().cloned()).collect::<BTreeSet<_>>();
        let mut external = BTreeMap::new();
        for file in files {
            let mut paths = ExternalPaths {
                file: &file.relative_path,
                resolver: &resolver,
                declared: &declared,
                found: BTreeMap::new(),
            };
            paths.visit_file(&file.syntax);
            if !paths.found.is_empty() {
                external.insert(file.relative_path.clone(), paths.found);
            }
        }
        let mut identity =
            Self { phantoms: phantoms.clone(), external, projections: BTreeMap::new() };
        let mut projections = BTreeMap::<String, BTreeSet<String>>::new();
        for file in files {
            let mut assigned = AssociatedTypes {
                names: identity.in_file(&file.relative_path),
                found: &mut projections,
            };
            assigned.visit_file(&file.syntax);
        }
        identity.projections = projections;
        identity
    }

    /// The types `file`'s paths name.
    pub(crate) fn in_file<'a>(&'a self, file: &'a str) -> FileTypes<'a> {
        FileTypes {
            phantoms: &self.phantoms,
            external: self.external.get(file),
            projections: &self.projections,
        }
    }
}

/// How one file's type paths name types.
#[derive(Clone, Copy)]
pub(crate) struct FileTypes<'a> {
    pub(crate) phantoms: &'a BTreeSet<String>,
    external: Option<&'a BTreeMap<PathKey, String>>,
    projections: &'a BTreeMap<String, BTreeSet<String>>,
}

impl<'a> FileTypes<'a> {
    /// The same names, with `phantoms` as the policy's phantom generics.
    pub(crate) fn with_phantoms(self, phantoms: &'a BTreeSet<String>) -> Self {
        Self { phantoms, ..self }
    }

    /// The types a projection of an associated type names: every type the
    /// workspace's trait impls give it. `None` for any other path. A
    /// projection is `<T as Trait>::Name`, or `T::Name` where `T` is a type
    /// parameter or `Self`, whose name is capitalized as a type's is.
    pub(crate) fn projection(&self, path: &syn::TypePath) -> Option<&'a BTreeSet<String>> {
        let last = path.path.segments.last()?;
        let projected = match &path.qself {
            Some(_) => true,
            None => {
                path.path.leading_colon.is_none()
                    && path.path.segments.len() == 2
                    && path.path.segments[0]
                        .ident
                        .to_string()
                        .starts_with(|first: char| first.is_ascii_uppercase())
            }
        };
        projected.then(|| self.projections.get(&last.ident.to_string())).flatten()
    }

    /// The name the rules know the type `path` names by.
    pub(crate) fn name(&self, path: &syn::Path) -> Option<String> {
        let last = path.segments.last()?;
        let start = last.ident.span().start();
        Some(
            self.external
                .and_then(|external| {
                    external.get(&(start.line, start.column, qualified_path(path)))
                })
                .cloned()
                .unwrap_or_else(|| last.ident.to_string()),
        )
    }
}

/// The types one file's trait impls give their associated types.
struct AssociatedTypes<'a, 'f> {
    names: FileTypes<'a>,
    found: &'f mut BTreeMap<String, BTreeSet<String>>,
}

impl<'ast> Visit<'ast> for AssociatedTypes<'_, '_> {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if node.trait_.is_some() {
            for item in &node.items {
                if let syn::ImplItem::Type(assigned) = item {
                    self.found
                        .entry(assigned.ident.to_string())
                        .or_default()
                        .extend(crate::syntax::type_names(&assigned.ty, self.names));
                }
            }
        }
        visit::visit_item_impl(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
        visit::visit_macro(self, node);
    }
}

/// The paths of one file naming an external type under a workspace type's
/// name: every type path, and every trait bound.
struct ExternalPaths<'a> {
    file: &'a str,
    resolver: &'a NameResolver,
    declared: &'a BTreeSet<String>,
    found: BTreeMap<PathKey, String>,
}

impl ExternalPaths<'_> {
    fn check(&mut self, path: &syn::Path) {
        let Some(last) = path.segments.last() else {
            return;
        };
        if !self.declared.contains(&last.ident.to_string()) {
            return;
        }
        let written = qualified_path(path);
        let Ok(resolved) = self.resolver.resolve_type(self.file, path.span(), &written) else {
            return;
        };
        if !self.resolver.types.contains(&resolved) {
            let start = last.ident.span().start();
            self.found.insert((start.line, start.column, written), resolved.join("::"));
        }
    }
}

impl<'ast> Visit<'ast> for ExternalPaths<'_> {
    fn visit_type_path(&mut self, node: &'ast syn::TypePath) {
        if node.qself.is_none() {
            self.check(&node.path);
        }
        visit::visit_type_path(self, node);
    }

    fn visit_trait_bound(&mut self, node: &'ast syn::TraitBound) {
        self.check(&node.path);
        visit::visit_trait_bound(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(body) = parse_macro_body(node) {
            body.visit(self);
        }
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
#[path = "type_identity_tests.rs"]
mod tests;
