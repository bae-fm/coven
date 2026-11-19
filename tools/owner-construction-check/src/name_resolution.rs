//! Resolve source names in their module and lexical scope before inspecting
//! a definition. External paths retain their identity; they never borrow a
//! workspace definition with the same final identifier.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::Visit;

use crate::module_sources::module_sources;
use crate::syntax::{path_names, RustFile};

mod collector;

use collector::Collector;

pub(crate) type ItemPath = Vec<String>;
type Position = (usize, usize);

/// What a name resolves to in a scope and namespace.
type Lookup = Result<ItemPath, ResolutionError>;

/// The lookups under way, by scope, name, whether lexical, and namespace:
/// one that comes back to itself has gone round a cycle of imports.
type Visiting = BTreeSet<(usize, String, bool, Namespace)>;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum Namespace {
    Type,
    Value,
    Macro,
}

/// The module whose code, and its descendants' code, may name a binding:
/// the module itself for a private one, its crate for a `pub(crate)` one,
/// and the empty path, every module, for a `pub` one.
type Visibility = ItemPath;

#[derive(Clone)]
enum Binding {
    Definition(ItemPath, Namespace, Visibility),
    Import(ItemPath, Visibility),
}

impl Binding {
    fn visible_to(&self, viewer: Option<&[String]>) -> bool {
        let visibility = match self {
            Binding::Definition(_, _, visibility) | Binding::Import(_, visibility) => visibility,
        };
        visible(visibility, viewer)
    }
}

/// A `use path::*`.
struct Glob {
    path: ItemPath,
    visibility: Visibility,
}

/// Whether code in the module `viewer` may name what has `visibility`; any
/// code may when there is no viewer.
fn visible(visibility: &[String], viewer: Option<&[String]>) -> bool {
    viewer.is_none_or(|viewer| viewer.starts_with(visibility))
}

struct Scope {
    module: ItemPath,
    identity: ItemPath,
    parent: Option<usize>,
    names: BTreeMap<String, Vec<Binding>>,
    globs: Vec<Glob>,
    /// The `macro_rules!` this scope defines, by name: indexes into
    /// `NameResolver::macros`.
    macros: BTreeMap<String, Vec<usize>>,
}

struct Region {
    start: Position,
    end: Position,
    scope: usize,
}

pub(crate) struct NameResolver {
    scopes: Vec<Scope>,
    modules: BTreeMap<ItemPath, usize>,
    module_parents: BTreeMap<ItemPath, ItemPath>,
    regions: BTreeMap<String, Vec<Region>>,
    pub(crate) types: BTreeSet<ItemPath>,
    enum_members: BTreeMap<ItemPath, BTreeSet<String>>,
    extern_crates: BTreeMap<String, BTreeMap<String, String>>,
    prelude: usize,
    /// Every `macro_rules!` definition.
    macros: Vec<MacroDefinition>,
    /// The definitions a path names: `#[macro_export]` ones at their crate's
    /// root, module-level ones in their module for a `use` to re-export.
    macro_paths: BTreeMap<ItemPath, Vec<usize>>,
    /// Modules declared `#[macro_use] mod child;`, whose definitions stay in
    /// scope in the module that declares them.
    macro_use_modules: BTreeMap<ItemPath, Vec<ItemPath>>,
    /// Each crate's `#[macro_use] extern crate` names, whose exported
    /// definitions are in scope everywhere in it.
    macro_use_crates: BTreeMap<String, Vec<String>>,
    /// For each scope, by its index, what the resolver resolved each name to
    /// there in each namespace (`name_in_scope`).
    lookups: Vec<RefCell<BTreeMap<(String, Namespace), Lookup>>>,
}

/// A `macro_rules!` definition and where it is.
pub(crate) struct MacroDefinition {
    pub(crate) name: String,
    pub(crate) file: String,
    position: Position,
    /// The crate the definition is in, which its `$crate` names.
    pub(crate) crate_name: String,
    pub(crate) definition: syn::Macro,
}

/// What a macro invocation's path names.
pub(crate) enum MacroLookup {
    /// A `macro_rules!` of the workspace, by its index (`macro_definition`).
    Workspace(usize),
    /// A macro of a dependency or the standard library.
    External,
    /// A name a workspace macro has, which no definition in scope answers.
    Unresolved(ResolutionError),
}

#[derive(Debug, Clone)]
pub(crate) enum ResolutionError {
    Missing,
    WrongNamespace,
    Ambiguous(BTreeSet<ItemPath>),
    ImportCycle,
}

impl std::fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "no definition or import in the caller's scope"),
            Self::WrongNamespace => write!(f, "the name exists in a different Rust namespace"),
            Self::Ambiguous(paths) => write!(
                f,
                "ambiguous paths: {}",
                paths.iter().map(|path| path.join("::")).collect::<Vec<_>>().join(", ")
            ),
            Self::ImportCycle => write!(f, "cyclic imports"),
        }
    }
}

impl NameResolver {
    pub(crate) fn collect(files: &[RustFile]) -> Self {
        let modules = module_sources(files);
        let mut resolver = Self {
            scopes: Vec::new(),
            modules: BTreeMap::new(),
            module_parents: BTreeMap::new(),
            regions: BTreeMap::new(),
            types: BTreeSet::new(),
            enum_members: BTreeMap::new(),
            extern_crates: BTreeMap::new(),
            prelude: 0,
            macros: Vec::new(),
            macro_paths: BTreeMap::new(),
            macro_use_modules: BTreeMap::new(),
            macro_use_crates: BTreeMap::new(),
            lookups: Vec::new(),
        };
        let prelude = resolver.module(&["<prelude>".into()]);
        resolver.prelude = prelude;
        let syntax = syn::parse_file(crate::rust_prelude::IMPORTS).expect("Rust prelude imports");
        Collector { resolver: &mut resolver, file: "<prelude>", scope: prelude }
            .visit_file(&syntax);
        for module in modules.values().flatten() {
            for end in 1..=module.len() {
                resolver.module(&module[..end]);
            }
        }
        for file in files {
            for module in modules.get(&file.relative_path).into_iter().flatten() {
                resolver
                    .extern_crates
                    .entry(module[0].clone())
                    .or_default()
                    .extend(file.extern_crates.clone());
                let scope = resolver.module(module);
                resolver.regions.entry(file.relative_path.clone()).or_default().push(Region {
                    start: (0, 0),
                    end: (usize::MAX, usize::MAX),
                    scope,
                });
                Collector { resolver: &mut resolver, file: &file.relative_path, scope }
                    .visit_file(&file.syntax);
            }
        }
        resolver.lookups =
            std::iter::repeat_with(RefCell::default).take(resolver.scopes.len()).collect();
        resolver
    }

    /// What the resolver resolves a name to in a scope and namespace. The
    /// resolver looks up each name once per scope and namespace. In a block
    /// that neither binds the name nor imports a glob, it resolves the name
    /// as the scope around the block does, so the blocks of one module share
    /// that module's lookup.
    fn name_in_scope(
        &self,
        scope: usize,
        name: &str,
        namespace: Namespace,
    ) -> Result<ItemPath, ResolutionError> {
        let key = (name.to_string(), namespace);
        if let Some(found) = self.lookups[scope].borrow().get(&key) {
            return found.clone();
        }
        let current = &self.scopes[scope];
        let found = match current.parent {
            Some(parent) if !current.names.contains_key(name) && current.globs.is_empty() => {
                self.name_in_scope(parent, name, namespace)
            }
            _ => self.named(scope, name, namespace),
        };
        self.lookups[scope].borrow_mut().insert(key, found.clone());
        found
    }

    /// What `name` names where code in `scope` uses it. A name nothing the
    /// checker reads names, in a scope an outside crate's glob brings names
    /// into, is one of the names that crate exports, since the code compiles:
    /// the glob's of the nearest scope that has one, as a glob in a block
    /// hides the names of the scopes around it.
    fn named(&self, scope: usize, name: &str, namespace: Namespace) -> Lookup {
        let found = self.lookup(scope, name, true, namespace, &mut BTreeSet::new(), None);
        let Err(ResolutionError::Missing) = found else {
            return found;
        };
        let mut current = Some(scope);
        while let Some(scope) = current {
            let mut outside = BTreeSet::new();
            self.outside_globs(scope, None, &mut BTreeSet::new(), &mut outside);
            if !outside.is_empty() {
                let members = outside.into_iter().map(|mut glob| {
                    glob.push(name.to_string());
                    glob
                });
                return unique(members.collect());
            }
            current = self.scopes[scope].parent;
        }
        found
    }

    /// The outside crates' modules whose globs bring names into `scope`:
    /// those of its own globs, and those of the workspace modules its globs
    /// bring in, as far as `viewer`, the module of the first glob, may name
    /// them.
    fn outside_globs(
        &self,
        scope: usize,
        viewer: Option<&[String]>,
        visited: &mut BTreeSet<usize>,
        outside: &mut BTreeSet<ItemPath>,
    ) {
        if !visited.insert(scope) {
            return;
        }
        let current = &self.scopes[scope];
        let glob_viewer = viewer.unwrap_or(&current.module);
        for glob in current.globs.iter().filter(|glob| visible(&glob.visibility, viewer)) {
            let target = self.resolve_in(scope, &glob.path, Namespace::Type, &mut BTreeSet::new());
            let Ok(target) = target else { continue };
            if self.enum_members.contains_key(&target) {
                continue;
            }
            match self.modules.get(&target) {
                Some(module) => self.outside_globs(*module, Some(glob_viewer), visited, outside),
                None => {
                    outside.insert(target);
                }
            }
        }
    }

    /// The scope of the module at `path`, which its parent names privately
    /// until its declaration says otherwise (`local_module`).
    fn module(&mut self, path: &[String]) -> usize {
        if let Some(scope) = self.modules.get(path) {
            return *scope;
        }
        let scope = self.scopes.len();
        self.scopes.push(Scope {
            module: path.to_vec(),
            identity: path.to_vec(),
            parent: None,
            names: BTreeMap::new(),
            globs: Vec::new(),
            macros: BTreeMap::new(),
        });
        self.modules.insert(path.to_vec(), scope);
        if let Some((name, parent_path)) = path.split_last().filter(|_| path.len() > 1) {
            let parent = self.module(parent_path);
            self.module_parents.insert(path.to_vec(), parent_path.to_vec());
            self.scopes[parent].names.entry(name.clone()).or_default().push(Binding::Definition(
                path.to_vec(),
                Namespace::Type,
                parent_path.to_vec(),
            ));
        }
        scope
    }

    /// The scope of the module `name` that `parent` declares, which code
    /// within `visibility` may name.
    fn local_module(&mut self, parent: usize, name: &str, visibility: Visibility) -> usize {
        let mut path = self.scopes[parent].identity.clone();
        path.push(name.to_string());
        if self.scopes[parent].parent.is_none() {
            let scope = self.module(&path);
            for binding in self.scopes[parent].names.get_mut(name).into_iter().flatten() {
                match binding {
                    Binding::Definition(declared, Namespace::Type, declared_visibility)
                        if *declared == path =>
                    {
                        *declared_visibility = visibility.clone();
                    }
                    _ => {}
                }
            }
            return scope;
        }
        let scope = self.scopes.len();
        self.scopes.push(Scope {
            module: path.clone(),
            identity: path.clone(),
            parent: None,
            names: BTreeMap::new(),
            globs: Vec::new(),
            macros: BTreeMap::new(),
        });
        self.modules.insert(path.clone(), scope);
        self.module_parents.insert(path.clone(), self.scopes[parent].module.clone());
        self.scopes[parent].names.entry(name.to_string()).or_default().push(Binding::Definition(
            path,
            Namespace::Type,
            visibility,
        ));
        scope
    }

    fn scope_at(&self, file: &str, span: Span) -> usize {
        let position = position(span.start());
        self.regions[file]
            .iter()
            .rev()
            .find(|region| region.start <= position && position <= region.end)
            .expect("file scope")
            .scope
    }

    /// The crate a position in a file is in.
    pub(crate) fn crate_at(&self, file: &str, span: Span) -> &str {
        &self.scopes[self.scope_at(file, span)].module[0]
    }

    /// The definition a macro invocation's path names. A single name is
    /// looked up first in textual scope, as rustc does: definitions earlier
    /// in the scopes around the invocation, in its ancestor modules, and in
    /// modules declared `#[macro_use]`; then as a path, through imports and
    /// `#[macro_export]`; then among the exports of `#[macro_use]` crates.
    pub(crate) fn resolve_macro(&self, file: &str, path: &syn::Path) -> MacroLookup {
        let span = path.segments.last().map_or_else(|| path.span(), |segment| segment.ident.span());
        let names = qualified_path(path);
        let single = match names.as_slice() {
            [name] => Some(name.as_str()),
            _ => None,
        };
        if let Some(found) = single.and_then(|name| self.textual_macro(file, span, name)) {
            return MacroLookup::Workspace(found);
        }
        let scope = self.scope_at(file, span);
        let resolved = self.resolve_in(scope, &names, Namespace::Macro, &mut BTreeSet::new());
        if let Ok(resolved) = &resolved {
            if let Some(found) = self.latest(self.macro_paths.get(resolved)) {
                return MacroLookup::Workspace(found);
            }
        }
        if let Some(name) = single {
            let crate_name = &self.scopes[scope].module[0];
            for used in self.macro_use_crates.get(crate_name).into_iter().flatten() {
                let target = self
                    .extern_crates
                    .get(crate_name)
                    .and_then(|names| names.get(used))
                    .unwrap_or(used);
                let exported = vec![target.clone(), name.to_string()];
                if let Some(found) = self.latest(self.macro_paths.get(&exported)) {
                    return MacroLookup::Workspace(found);
                }
            }
        }
        let named_like_ours = names.last().is_some_and(|last| self.names_a_macro(last));
        match resolved {
            Ok(_) if !named_like_ours => MacroLookup::External,
            Err(_) if !named_like_ours => MacroLookup::External,
            Ok(_) => MacroLookup::Unresolved(ResolutionError::Missing),
            Err(error) => MacroLookup::Unresolved(error),
        }
    }

    pub(crate) fn macro_definition(&self, index: usize) -> &MacroDefinition {
        &self.macros[index]
    }

    /// Whether a workspace `macro_rules!` has this name.
    pub(crate) fn names_a_macro(&self, name: &str) -> bool {
        self.macros.iter().any(|definition| definition.name == name)
    }

    /// The definition among `found` that comes last in its file.
    fn latest(&self, found: Option<&Vec<usize>>) -> Option<usize> {
        found?.iter().copied().max_by_key(|index| self.macros[*index].position)
    }

    fn textual_macro(&self, file: &str, span: Span, name: &str) -> Option<usize> {
        let position = position(span.start());
        let mut scope = self.scope_at(file, span);
        loop {
            if let Some(found) = self.visible_macro(scope, name, file, position) {
                return Some(found);
            }
            match self.scopes[scope].parent {
                Some(parent) => scope = parent,
                None => break,
            }
        }
        let mut module = self.scopes[scope].module.clone();
        while let Some(parent) = self.module_parents.get(&module) {
            if let Some(found) = self.visible_macro(self.modules[parent], name, file, position) {
                return Some(found);
            }
            module = parent.clone();
        }
        None
    }

    /// A definition of `name` in `scope`, or in a module it declares
    /// `#[macro_use]`, that comes before `position` when it is in `file`.
    fn visible_macro(
        &self,
        scope: usize,
        name: &str,
        file: &str,
        position: Position,
    ) -> Option<usize> {
        let mut candidates = self.scopes[scope].macros.get(name).cloned().unwrap_or_default();
        let mut pending =
            self.macro_use_modules.get(&self.scopes[scope].identity).cloned().unwrap_or_default();
        while let Some(child) = pending.pop() {
            if let Some(child_scope) = self.modules.get(&child) {
                candidates.extend(
                    self.scopes[*child_scope].macros.get(name).into_iter().flatten().copied(),
                );
            }
            pending.extend(self.macro_use_modules.get(&child).into_iter().flatten().cloned());
        }
        candidates
            .into_iter()
            .filter(|index| {
                let definition = &self.macros[*index];
                definition.file != file || definition.position < position
            })
            .max_by_key(|index| {
                let definition = &self.macros[*index];
                (definition.file == file, definition.position)
            })
    }

    /// The value a path names where `span` is.
    pub(crate) fn resolve(
        &self,
        file: &str,
        span: Span,
        path: &[String],
    ) -> Result<ItemPath, ResolutionError> {
        self.resolve_where(file, span, path, Namespace::Value)
    }

    /// The type a path names where `span` is.
    pub(crate) fn resolve_type(
        &self,
        file: &str,
        span: Span,
        path: &[String],
    ) -> Result<ItemPath, ResolutionError> {
        self.resolve_where(file, span, path, Namespace::Type)
    }

    /// The item in `namespace` a path names where `span` is. A path that
    /// starts with a name resolves that name once per scope
    /// (`name_in_scope`).
    fn resolve_where(
        &self,
        file: &str,
        span: Span,
        path: &[String],
        namespace: Namespace,
    ) -> Lookup {
        let scope = self.scope_at(file, span);
        match path.split_first() {
            Some((first, rest)) if !matches!(first.as_str(), "" | "crate" | "self" | "super") => {
                let first_namespace = if rest.is_empty() { namespace } else { Namespace::Type };
                let base = self.name_in_scope(scope, first, first_namespace)?;
                self.descend(base, rest, namespace, &mut BTreeSet::new())
            }
            _ => self.resolve_in(scope, path, namespace, &mut BTreeSet::new()),
        }
    }

    fn resolve_in(
        &self,
        scope: usize,
        path: &[String],
        namespace: Namespace,
        visiting: &mut Visiting,
    ) -> Result<ItemPath, ResolutionError> {
        let Some((first, rest)) = path.split_first() else {
            return Err(ResolutionError::Missing);
        };
        let module = &self.scopes[scope].module;
        if let Some((base, rest)) = self.module_prefix(module, path)? {
            return self.descend(base, rest, namespace, visiting);
        }
        let (base, rest) = match first.as_str() {
            "" => {
                let (root, rest) = rest.split_first().ok_or(ResolutionError::Missing)?;
                (self.external(module, root).ok_or(ResolutionError::Missing)?, rest)
            }
            _ => (
                self.lookup(
                    scope,
                    first,
                    true,
                    if rest.is_empty() { namespace } else { Namespace::Type },
                    visiting,
                    None,
                )?,
                rest,
            ),
        };
        self.descend(base, rest, namespace, visiting)
    }

    /// The module a path starting with `crate`, `self` or `super` names
    /// from code in `module`, and the rest of the path; none for a path
    /// that starts otherwise. A `super` past the crate's root names no
    /// module.
    fn module_prefix<'p>(
        &self,
        module: &[String],
        path: &'p [String],
    ) -> Result<Option<(ItemPath, &'p [String])>, ResolutionError> {
        let Some((first, rest)) = path.split_first() else { return Ok(None) };
        match first.as_str() {
            "crate" => Ok(Some((vec![module[0].clone()], rest))),
            "self" => Ok(Some((module.to_vec(), rest))),
            "super" => {
                let count = path.iter().take_while(|part| part.as_str() == "super").count();
                let mut parent = module;
                for _ in 0..count {
                    parent = self.module_parents.get(parent).ok_or(ResolutionError::Missing)?;
                }
                Ok(Some((parent.to_vec(), &path[count..])))
            }
            _ => Ok(None),
        }
    }

    fn descend(
        &self,
        mut base: ItemPath,
        rest: &[String],
        namespace: Namespace,
        visiting: &mut Visiting,
    ) -> Result<ItemPath, ResolutionError> {
        for (index, name) in rest.iter().enumerate() {
            if let Some(scope) = self.modules.get(&base) {
                base = self.lookup(
                    *scope,
                    name,
                    false,
                    if index + 1 == rest.len() { namespace } else { Namespace::Type },
                    visiting,
                    None,
                )?;
            } else {
                base.push(name.clone());
            }
        }
        Ok(base)
    }

    /// What `name` names in `scope` and `namespace`: through the scope's own
    /// bindings, then its globs, and when `lexical`, in the scopes around it.
    /// With a `viewer`, the lookup is a glob's in the module `viewer`, which
    /// brings in only the bindings and globs that module may name.
    fn lookup(
        &self,
        scope: usize,
        name: &str,
        lexical: bool,
        namespace: Namespace,
        visiting: &mut Visiting,
        viewer: Option<&[String]>,
    ) -> Result<ItemPath, ResolutionError> {
        let key = (scope, name.to_string(), lexical, namespace);
        if !visiting.insert(key.clone()) {
            let definitions = self.definitions_of(scope, name, namespace, viewer);
            return if !definitions.is_empty() {
                unique(definitions)
            } else if lexical {
                // A glob's own path never names through the glob, as
                // `use lofty::prelude::*` looking up `lofty` would: the name
                // is what the scopes around it, the dependencies and the
                // prelude call it, as rustc resolves it.
                self.enclosing(scope, name, namespace, visiting, ResolutionError::ImportCycle)
            } else {
                Err(ResolutionError::ImportCycle)
            };
        }
        let result = self.lookup_inner(scope, name, lexical, namespace, visiting, viewer);
        visiting.remove(&key);
        result
    }

    fn lookup_inner(
        &self,
        scope: usize,
        name: &str,
        lexical: bool,
        namespace: Namespace,
        visiting: &mut Visiting,
        viewer: Option<&[String]>,
    ) -> Result<ItemPath, ResolutionError> {
        let bindings = self.scopes[scope].names.get(name).into_iter().flatten();
        let bindings = bindings.filter(|binding| binding.visible_to(viewer)).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        for binding in &bindings {
            match binding {
                Binding::Definition(path, kind, _) if *kind == namespace => {
                    paths.insert(path.clone());
                }
                Binding::Definition(..) => {}
                Binding::Import(path, _) => {
                    match self.import(scope, name, path, namespace, visiting) {
                        Ok(path) => {
                            paths.insert(path);
                        }
                        Err(ResolutionError::WrongNamespace) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        if !paths.is_empty() {
            return unique(paths);
        }
        match self.beyond_bindings(scope, name, lexical, namespace, visiting, viewer) {
            Err(ResolutionError::Missing) if !bindings.is_empty() => {
                Err(ResolutionError::WrongNamespace)
            }
            found => found,
        }
    }

    /// The paths `scope` defines `name` as in `namespace`, those `viewer`
    /// may name.
    fn definitions_of(
        &self,
        scope: usize,
        name: &str,
        namespace: Namespace,
        viewer: Option<&[String]>,
    ) -> BTreeSet<ItemPath> {
        let bindings = self.scopes[scope].names.get(name).into_iter().flatten();
        bindings
            .filter(|binding| binding.visible_to(viewer))
            .filter_map(|binding| match binding {
                Binding::Definition(path, kind, _) if *kind == namespace => Some(path.clone()),
                _ => None,
            })
            .collect()
    }

    /// What the import of `path` that `scope` binds as `name` names in
    /// `namespace`. An import whose path starts with its own name, as
    /// `use rusqlite;` or a `macro_rules!`'s re-export `use name;` does,
    /// never names itself: it names what the scope's definitions, its globs
    /// and the scopes around it call that name, and nothing in a namespace
    /// where they call it nothing.
    fn import(
        &self,
        scope: usize,
        name: &str,
        path: &[String],
        namespace: Namespace,
        visiting: &mut Visiting,
    ) -> Result<ItemPath, ResolutionError> {
        let Some(rest) = path.strip_prefix(&[name.to_string()]) else {
            return self.resolve_in(scope, path, namespace, visiting);
        };
        let first = if rest.is_empty() { namespace } else { Namespace::Type };
        let definitions = self.definitions_of(scope, name, first, None);
        let base = if definitions.is_empty() {
            match self.beyond_bindings(scope, name, true, first, visiting, None) {
                Err(ResolutionError::Missing) => Err(ResolutionError::WrongNamespace),
                found => found,
            }
        } else {
            unique(definitions)
        }?;
        self.descend(base, rest, namespace, visiting)
    }

    /// What `name` names in `scope` beyond the scope's own bindings: through
    /// its globs, then, when `lexical`, in the scopes around it, the
    /// dependencies and the prelude. With a `viewer`, only the globs that
    /// module may name count, and each brings in only what it may name.
    fn beyond_bindings(
        &self,
        scope: usize,
        name: &str,
        lexical: bool,
        namespace: Namespace,
        visiting: &mut Visiting,
        viewer: Option<&[String]>,
    ) -> Result<ItemPath, ResolutionError> {
        let current = &self.scopes[scope];
        let glob_viewer = viewer.unwrap_or(&current.module);
        let mut paths = BTreeSet::new();
        for glob in current.globs.iter().filter(|glob| visible(&glob.visibility, viewer)) {
            let target = match self.resolve_in(scope, &glob.path, Namespace::Type, visiting) {
                Ok(target) => target,
                Err(ResolutionError::ImportCycle) => continue,
                Err(error) => return Err(error),
            };
            match self.glob_member(target, name, namespace, visiting, glob_viewer) {
                Ok(path) => {
                    paths.insert(path);
                }
                // A glob's absent name is not a failed call. Another import
                // may supply it; multiple supplied definitions are ambiguous.
                Err(
                    ResolutionError::Missing
                    | ResolutionError::WrongNamespace
                    | ResolutionError::ImportCycle,
                ) => {}
                Err(error) => return Err(error),
            }
        }
        if !paths.is_empty() {
            return unique(paths);
        }
        if !lexical {
            return Err(ResolutionError::Missing);
        }
        self.enclosing(scope, name, namespace, visiting, ResolutionError::Missing)
    }

    /// What a glob of `target` in the module `viewer` brings in as `name`:
    /// an enum's variant, or a binding of a workspace module that `viewer`
    /// may name. An outside crate's module brings in what that crate
    /// exports, which the checker doesn't read: no name any other import or
    /// definition has (`named`).
    fn glob_member(
        &self,
        target: ItemPath,
        name: &str,
        namespace: Namespace,
        visiting: &mut Visiting,
        viewer: &[String],
    ) -> Result<ItemPath, ResolutionError> {
        if let Some(members) = self.enum_members.get(&target) {
            if !members.contains(name) {
                return Err(ResolutionError::Missing);
            }
            let mut variant = target;
            variant.push(name.to_string());
            return Ok(variant);
        }
        let Some(module) = self.modules.get(&target) else {
            return Err(ResolutionError::Missing);
        };
        self.lookup(*module, name, false, namespace, visiting, Some(viewer))
    }

    /// What `name` names beyond `scope`'s own bindings: in the scopes
    /// around it, then among the dependencies, then in the prelude; else
    /// `missing`.
    fn enclosing(
        &self,
        scope: usize,
        name: &str,
        namespace: Namespace,
        visiting: &mut Visiting,
        missing: ResolutionError,
    ) -> Result<ItemPath, ResolutionError> {
        let current = &self.scopes[scope];
        if let Some(parent) = current.parent {
            return self.lookup(parent, name, true, namespace, visiting, None);
        }
        if namespace == Namespace::Type {
            if let Some(path) = self.external(&current.module, name) {
                return Ok(path);
            }
        }
        if scope != self.prelude {
            return match self.lookup(self.prelude, name, false, namespace, visiting, None) {
                Err(ResolutionError::Missing | ResolutionError::WrongNamespace) => Err(missing),
                result => result,
            };
        }
        Err(missing)
    }

    fn external(&self, module: &[String], name: &str) -> Option<ItemPath> {
        if let Some(target) = self.extern_crates.get(&module[0]).and_then(|names| names.get(name)) {
            return Some(vec![target.clone()]);
        }
        // Rust's implicit library crates participate in name resolution;
        // they do not grant construction authority.
        if self.modules.contains_key(&vec![name.to_string()])
            || matches!(name, "std" | "core" | "alloc")
        {
            return Some(vec![name.to_string()]);
        }
        None
    }
}

fn unique(mut paths: BTreeSet<ItemPath>) -> Result<ItemPath, ResolutionError> {
    match paths.len() {
        0 => Err(ResolutionError::Missing),
        1 => Ok(paths.pop_first().expect("one resolved path")),
        _ => Err(ResolutionError::Ambiguous(paths)),
    }
}

fn position(location: proc_macro2::LineColumn) -> Position {
    (location.line, location.column)
}

pub(crate) fn qualified_path(path: &syn::Path) -> ItemPath {
    let mut names = path_names(path);
    if path.leading_colon.is_some() {
        names.insert(0, String::new());
    }
    names
}
