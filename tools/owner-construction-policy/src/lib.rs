//! The policy file (Appendix F.3): what the checked workspace says about
//! itself. The workspace keeps it at its root as `owner-construction-check.toml`:
//! the rules it runs, its crates in dependency order, where each capability
//! lives, the construction and task-start roots, and the types that are
//! capabilities, handles, owners, tasks or plain values. Two tools read it,
//! each for the rules it runs: the structural checker
//! (`tools/owner-construction-check`), which reads the syntax tree, and the
//! typed lints (`tools/typed-lints`), which ask the compiler for types.
//! Neither tool's source names any of it; each workspace supplies its own.
//!
//! Each tool holds the rows it reads against the workspace, so a row naming
//! something that moved fails instead of covering nothing: the checker before
//! any rule runs (its `policy_check.rs`), and a typed lint when it resolves the
//! row's path in the crate the path names.
//!
//! The rows of the owner rules and the read rule, and each database domain's
//! owner, name each type by its path:
//! the crate as Rust names it, its modules and the type,
//! `coven_foundation::clock::Clock` or `std::sync::Arc`
//! ([`Policy::owner_type_rows`], [`ReadShapes`]). A dependency's type goes by
//! the path its crate gives it, a re-export's included. The other rows name a
//! type by its name alone.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

mod read;

pub use read::ReadShapes;

/// The policy file's name, at the root of the workspace it describes.
pub const FILE_NAME: &str = "owner-construction-check.toml";

/// rusqlite's raw handles, each by the path that defines it and as findings
/// name it: only the homes of the database's capability hold one.
pub const RAW_SQLITE_HANDLES: [(&str, &str); 4] = [
    ("rusqlite::Connection", "connection"),
    ("rusqlite::Transaction", "transaction"),
    ("rusqlite::Savepoint", "savepoint"),
    ("rusqlite::session::Session", "session"),
];

/// The groups of rules a policy selects. A workspace runs exactly the ones it
/// lists, so it can adopt them one at a time.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Rule {
    /// Appendix F.1: the crate order, and dependency versions set once in the
    /// workspace (`crate_dependencies.rs`).
    CrateDependencies,
    /// Appendix F.2: each capability used directly only in its homes, and
    /// background work starts only at composition roots (`capability_boundaries.rs`).
    CapabilityBoundaries,
    /// Appendix F.2: raw SQLite handles and SQL on the database's internal
    /// tables stay in the database's homes, and each domain's tables are
    /// written and created, and tested, only in its homes
    /// (`database_boundary.rs`); no type only passes its calls on to a
    /// domain's owner (`domain_owners.rs`).
    DatabaseBoundary,
    /// Appendix F.3: an owner never hands out what it holds, only its owner
    /// and what holds that owner hand out a handle, shared state leaves its
    /// owner only inside one, and what a task makes shared for its operation
    /// stays with tasks. The typed lints `clippy::owner_hands_out_dependency`,
    /// `clippy::handle_not_from_its_owner`, `clippy::shared_state_outside_handle`
    /// and `clippy::task_state_outside_task` check it (`tools/typed-lints`),
    /// not the checker.
    OwnerDependencies,
    /// Appendix F.2 and F.3: no method but a constructor takes a
    /// construction-only capability or a handle. The typed lint
    /// `clippy::method_takes_capability` checks it (`tools/typed-lints`),
    /// not the checker.
    RetainedCapabilityParameters,
    /// Appendix F.3: no struct built only to be destructured
    /// (`component_bundles.rs`).
    ComponentBundles,
    /// Appendix F.3: owners, construction-only capabilities and handles are
    /// built only at composition roots. The typed lint
    /// `clippy::built_outside_composition_root` checks it
    /// (`tools/typed-lints`), not the checker.
    CapabilityConstruction,
    /// Appendix F.2: errors are typed, never strings (`typed_errors.rs`).
    TypedErrors,
    /// Appendix F.5: visibility, file length and test layout (`conventions.rs`).
    Conventions,
}

impl Rule {
    /// Whether the checker reads the crates' code for the rule
    /// (`crate_code.rs`): every rule it runs but the crate dependencies, which
    /// read manifests, and the conventions, which read every file as written.
    pub fn reads_crate_code(self) -> bool {
        !matches!(
            self,
            Rule::CrateDependencies
                | Rule::Conventions
                | Rule::OwnerDependencies
                | Rule::CapabilityConstruction
                | Rule::RetainedCapabilityParameters
        )
    }
}

/// The policy file's rows.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// The rule groups this workspace runs.
    #[serde(default)]
    pub rules: BTreeSet<Rule>,
    /// Workspace members that answer only to the conventions, such as this
    /// checker. Every other member is a crate and answers to every rule.
    #[serde(default)]
    pub tools: BTreeSet<String>,
    /// Cargo features that only tests turn on. Every rule counts code that
    /// compiles only under `test` or one of these features
    /// (`cfg(feature = "test-utils")`, `cfg(any(test, feature = "test-utils"))`)
    /// as test code, as it counts `cfg(test)` code (`test_code.rs`). With
    /// none, only `cfg(test)` and `#[test]` code is test code.
    #[serde(default)]
    pub test_support_features: Vec<String>,
    /// The crate whose root re-exports the API and keeps every module private
    /// (Appendix F.5).
    #[serde(default)]
    pub api_crate: Option<String>,
    /// Appendix F.1: the workspace's crates in dependency order. A crate depends
    /// only on crates earlier in this list.
    #[serde(default)]
    pub crate_order: Vec<String>,
    /// Appendix F.1: pairs of crates in `crate_order` that never depend on each
    /// other in either direction.
    #[serde(default)]
    pub separated_crates: Vec<(String, String)>,
    /// Appendix F.2: each capability, the one place that uses it directly, and
    /// what using it directly looks like in source.
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    /// The SQLite capability and the database's tables, by the domain that
    /// writes them.
    #[serde(default)]
    pub database: Option<Database>,
    /// Every callable allowed to construct owners or start background work (Appendix F.3),
    /// including owner constructors; test code needs none (`test_code.rs`).
    /// The file lists them by source file: `Type::method`, or a free
    /// function's or a static's name; for a static, the code that builds its
    /// value is the root.
    #[serde(default, deserialize_with = "composition_roots")]
    pub composition_roots: Vec<CompositionRoot>,
    /// Runtime acquisition and background work starts require an entry in
    /// `composition_roots`.
    #[serde(default)]
    pub background_starts: Vec<Gate>,
    /// Receiver methods through which an owner may derive a capability from the
    /// one it was given, such as a directory naming one of its files. The file
    /// lists them by source file, the product by path:
    /// `"Type::method" = "app_foundation::files::AtomicFile"`.
    #[serde(default, deserialize_with = "capability_factories")]
    pub capability_factories: Vec<CapabilityFactory>,
    /// Types that hold a capability, by path. A type holding one of these, or
    /// holding an owner, is an owner.
    #[serde(default)]
    pub capability_types: BTreeSet<String>,
    /// Shared in-memory state (Appendix F.2), by path, such as a lock or a
    /// channel's end, which the standard library and the runtime declare: whoever holds
    /// one shares it with the others that do. A type holding one is an owner,
    /// as one holding a capability is, and it leaves its owner only inside a
    /// handle: no method crossing its module returns one, held or new.
    #[serde(default)]
    pub shared_state_types: BTreeSet<String>,
    /// In-memory state its holder keeps to itself while it holds it by
    /// value, by path, as an owner keeps a lock no one else reaches. Held through one
    /// of `shared_pointer_types` or a reference, by itself or inside a type
    /// that holds it by value, it is shared state, as `shared_state_types`
    /// are: `Arc<Mutex<State>>`, or `Arc<Inner>` where `Inner` holds a
    /// `Mutex`.
    #[serde(default)]
    pub private_state_types: BTreeSet<String>,
    /// Pointers whose every holder shares the value they point at, by path,
    /// such as `std::sync::Arc`: what one holds through them is shared with
    /// the others.
    #[serde(default)]
    pub shared_pointer_types: BTreeSet<String>,
    /// Capability interfaces, by path. Every workspace implementation outside
    /// test code is construction-only, including generic implementations.
    #[serde(default)]
    pub capability_traits: BTreeSet<String>,
    /// Handles (Appendix F.3), each with the type that makes it, both by path:
    /// `"sqlive::LiveQueryRequests" = "sqlive::LiveQuery"`. A handle is a capability its owner
    /// makes from its own state to hand out: only that owner (or the handle,
    /// cloning itself) makes one; that owner returns one, and so does an
    /// owner holding it, at any depth, passing up a fresh one, but nothing
    /// holding the handle itself; and a holder receives one only when it is
    /// built or as a free function's argument.
    #[serde(default)]
    pub handles: BTreeMap<String, String>,
    /// Raw capabilities fixed when the owner graph is built, which have no
    /// capability trait, by path. These and the implementations of `capability_traits`
    /// may be acquired only at composition roots, and accepted as parameters
    /// only by constructors and composition roots.
    #[serde(default)]
    pub construction_only_capability_types: BTreeSet<String>,
    /// Values, configuration and proofs that name a capability in a field but
    /// do not own its lifetime, by path.
    #[serde(default)]
    pub non_owner_types: BTreeSet<String>,
    /// API namespaces that borrow an owner without becoming one, by path.
    #[serde(default)]
    pub borrowed_facade_types: BTreeSet<String>,
    /// Tasks (Appendix F.3), by path: values that live for one piece of work
    /// and are then dropped.
    #[serde(default)]
    pub task_types: BTreeSet<String>,
    /// Dependencies an owner keeps to itself, by path; returning one it
    /// retains leaks it. The raw SQLite handles are always included.
    #[serde(default)]
    pub internal_dependency_types: BTreeSet<String>,
    /// Sessions that borrow internal dependencies for closed work, by path.
    /// One declared at a crate root would expose those dependencies to every
    /// module of the crate.
    #[serde(default)]
    pub closed_session_types: BTreeSet<String>,
    /// Dependencies whose library, as source code names it, is not their
    /// package's name with `-` as `_`: `"coreaudio-rs" = "coreaudio"`. The
    /// checker reads the workspace's manifests, not its dependencies', so a
    /// library's own name reaches it only here.
    #[serde(default)]
    pub library_names: BTreeMap<String, String>,
    /// The read rule's rows, which the typed lints of the read rule read
    /// when they are here.
    #[serde(default)]
    pub read_shapes: Option<ReadShapes>,
    /// Where the async functions uniffi exports run their work, which the
    /// typed lint `clippy::async_export_off_runtime` reads.
    #[serde(default)]
    pub async_exports: Option<AsyncExports>,
}

/// An async function that uniffi exports runs its work on a runtime that its
/// owner owns, never on the executor of the language that called it: its body
/// calls one of `runs`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsyncExports {
    /// The functions that run an operation on such a runtime, by the path
    /// that defines each, `app_bridge::runtime::Runtime::run`: the crate's
    /// name, its modules, and for a method its type.
    pub runs: Vec<String>,
}

/// `(source file, type, method)`: a callable allowed to construct owners or
/// start background work. A free function or a static has `<free>` in the type position.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct CompositionRoot {
    /// The source file.
    pub path: String,
    /// The type whose method the root is, or `<free>`.
    pub owner: String,
    /// The method, or the free function's or static's name.
    pub method: String,
}

/// `(source file, owner, receiver method, capability)`: an owner may derive
/// this capability from the one it was given. This grants no authority to
/// construct unrelated capabilities.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct CapabilityFactory {
    /// The source file.
    pub path: String,
    /// The owner whose method derives the capability.
    pub owner: String,
    /// The receiver method.
    pub method: String,
    /// The capability the method derives, by path.
    pub product: String,
}

/// A capability (Appendix F.2): where code uses it directly, and what using
/// it directly looks like in source.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    /// The capability's name, as findings and other rows name it.
    pub name: String,
    /// Path prefixes, relative to the workspace root, where the capability is
    /// used directly. A prefix ending in `/` names a directory.
    #[serde(default)]
    pub homes: Vec<String>,
    /// The ways of using the capability directly.
    #[serde(default)]
    pub gates: Vec<Gate>,
    /// Whether the rule reads the crates' test code for this capability too.
    /// Test code in a home, in a file home's tests (`<name>_tests.rs` and the
    /// `<name>_tests/` directory beside it) or in one of `test_homes` may use
    /// the capability directly.
    #[serde(default)]
    pub tests: bool,
    /// Source files, and directories (ending in `/`) of them, where test code
    /// may use the capability directly besides the homes.
    #[serde(default)]
    pub test_homes: Vec<String>,
    /// The direct uses outside the homes that the workspace has today, by
    /// file and gate kind; they only shrink (`capability_boundaries.rs`).
    #[serde(default)]
    pub frozen: Vec<FrozenUses>,
    /// Matches of a gate that use no capability, such as a `metadata` method
    /// of a value that is no path, by the item that holds them.
    #[serde(default)]
    pub allowed: Vec<AllowedUses>,
}

impl Capability {
    /// Whether production code in `path` may use the capability directly.
    pub fn holds(&self, path: &str) -> bool {
        self.homes.iter().any(|home| path.starts_with(home.as_str()))
    }

    /// Whether test code in `path` may use the capability directly: a
    /// home, a file home's tests, or a test home.
    pub fn holds_tests(&self, path: &str) -> bool {
        let tests_of_home =
            self.homes.iter().filter_map(|home| home.strip_suffix(".rs")).any(|stem| {
                path == format!("{stem}_tests.rs") || path.starts_with(&format!("{stem}_tests/"))
            });
        let test_home = self.test_homes.iter().any(|home| home_holds(home, path));
        self.holds(path) || tests_of_home || test_home
    }
}

/// `count` direct uses of a capability through the gate of kind `gate` in
/// `path`, and what the code does there.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenUses {
    /// The source file.
    pub path: String,
    /// The gate's kind.
    pub gate: String,
    /// How many uses the file holds.
    pub count: usize,
    /// What the code does there.
    pub finding: String,
}

/// `count` matches of the gate of kind `gate` in `item` of `path` that use
/// no capability, and why. `item` is the innermost function, method
/// (`Type::method`), static or const that holds them.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedUses {
    /// The source file.
    pub path: String,
    /// The item that holds the matches.
    pub item: String,
    /// The gate's kind.
    pub gate: String,
    /// How many matches the item holds.
    pub count: usize,
    /// Why the matches use no capability.
    pub reason: String,
}

/// One way of using a capability directly: naming any of `crates`, writing a
/// path containing any of `paths` as adjacent segments, calling a function
/// whose path holds any of `calls`, or calling a method named in `methods`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gate {
    /// What the gate finds, as findings name it.
    pub kind: String,
    /// Crates, as Rust names them, that only the homes name.
    #[serde(default)]
    pub crates: Vec<String>,
    /// Written as Rust paths, `std::fs`.
    #[serde(default, rename = "paths", deserialize_with = "rust_paths")]
    pub path_patterns: Vec<Vec<String>>,
    /// Written as Rust paths, `std::fs`. A function code calls or passes as
    /// a value matches when the path it resolves to through its file's
    /// imports and scopes holds one as adjacent segments; an import or a
    /// type never does. A path that resolves to nothing matches as written,
    /// unless it is a single name, which is a local binding.
    #[serde(default, rename = "calls", deserialize_with = "rust_paths")]
    pub call_patterns: Vec<Vec<String>>,
    /// Method names whose receiver-call form (`x.name(...)`) is gated; `syn`
    /// does not surface a method call as a path.
    #[serde(default, rename = "methods")]
    pub method_patterns: Vec<String>,
}

/// The SQLite capability and the database's own tables: outside the named
/// capability's homes, nothing names a raw rusqlite handle or writes SQL on
/// one of the schema module's tables. The schema's tables, which the domains
/// place, are the schema module's and those a `CREATE TABLE` in the crates'
/// `.sql` files makes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Database {
    /// The capability, in `capabilities`, whose homes hold the raw handles.
    pub capability: String,
    /// The schema module's root file. The module is that file and every
    /// module beneath it, wherever `#[path]` puts its file. Each
    /// `$visit!(table, …)` inside one of `table_macros`, top-level
    /// `macro_rules!` in that module, declares a table whose SQL only the
    /// database's homes write.
    #[serde(default)]
    pub schema: Option<String>,
    /// The schema module's macros that list its tables.
    #[serde(default)]
    pub table_macros: Vec<String>,
    /// The tables, grouped by the part of the workspace that writes them.
    #[serde(default)]
    pub domains: Vec<Domain>,
    /// The types an open write is: a function taking one runs inside a write
    /// its caller opened. A domain's tables are written only in its owner's
    /// methods and in functions and closures taking one of these
    /// (`database_boundary.rs`). With none, the rule reads no write path.
    #[serde(default)]
    pub write_types: Vec<String>,
    /// Public callables of the database's homes that take a raw database
    /// handle, each with the reason it may. A callable listed here is exempt
    /// from the parameter rule only; returning a handle stays refused.
    #[serde(default)]
    pub raw_handle_parameters: Vec<RawHandleParameter>,
    /// The columns the SQL only ever counts up that may, each with why. A
    /// live query runs again when what it read changes, so any other such
    /// column is a finding (`counters.rs`).
    #[serde(default)]
    pub counters: Vec<CounterColumn>,
}

/// A column the SQL only ever counts up, or resets, and why it may: it
/// numbers something, as a sequence does, rather than making a live query
/// run again.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CounterColumn {
    /// The column, as `table.column`.
    pub column: String,
    /// What the count is for.
    pub reason: String,
}

/// A public callable that takes a raw database handle. The policy file states
/// why in a comment above its row.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawHandleParameter {
    /// The source file that declares the callable.
    pub path: String,
    /// The callable, as `Owner::name` for a method or `name` for a function.
    pub callable: String,
}

/// A domain: tables whose schema and writes only its homes hold. SQL
/// elsewhere may read them; writing one — `INSERT`, `UPDATE`, `DELETE` or
/// `REPLACE`, in a statement or in a trigger's body, or through a view whose
/// `INSTEAD OF` triggers write it — is the domain's own work, and so are
/// their `CREATE` statements and the tests that write them. The domain keeps
/// its views too: those it names, and those whose `INSTEAD OF` triggers write
/// only its tables (`database_boundary.rs`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    /// The domain's name, as findings name it.
    pub name: String,
    /// Path prefixes, relative to the workspace root, where SQL writes and
    /// creates the domain's tables. A prefix ending in `/` names a directory.
    pub homes: Vec<String>,
    /// Table names, as SQL writes them; they compare without case.
    pub tables: Vec<String>,
    /// Views the domain keeps whose `INSTEAD OF` triggers don't write its
    /// tables alone, as SQL writes them; they compare without case.
    #[serde(default)]
    pub views: Vec<String>,
    /// The owner that does the domain's work, by its path: the type holding
    /// what the domain's writes go through. The typed lint
    /// `clippy::domain_owner_is_no_owner` holds it to being an owner, and the
    /// checker reads it by name (`domain_owners.rs`).
    pub owner: String,
}

impl Domain {
    /// Whether a file is in one of the domain's homes. A home file's tests,
    /// `<name>_tests.rs` beside it, are in its home.
    pub fn holds(&self, path: &str) -> bool {
        let tested = path.strip_suffix("_tests.rs").map(|source| format!("{source}.rs"));
        self.homes
            .iter()
            .any(|home| path.starts_with(home.as_str()) || tested.as_deref() == Some(home.as_str()))
    }
}

/// Why the policy file could not be read.
#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    /// The file could not be read.
    #[error("read {}: {source}", path.display())]
    Read {
        /// The policy file.
        path: PathBuf,
        /// Why reading it failed.
        #[source]
        source: std::io::Error,
    },
    /// The file holds no policy.
    #[error("parse {}: {source}", path.display())]
    Parse {
        /// The policy file.
        path: PathBuf,
        /// Why parsing it failed.
        #[source]
        source: toml::de::Error,
    },
}

impl Policy {
    /// The policy at the root of the workspace at `root`.
    pub fn read(root: &Path) -> Result<Policy, PolicyError> {
        let path = root.join(FILE_NAME);
        let text = std::fs::read_to_string(&path)
            .map_err(|source| PolicyError::Read { path: path.clone(), source })?;
        toml::from_str(&text).map_err(|source| PolicyError::Parse { path, source })
    }

    /// Whether the workspace runs `rule`.
    pub fn selects(&self, rule: Rule) -> bool {
        self.rules.contains(&rule)
    }

    /// Whether a workspace member answers only to the conventions.
    pub fn is_tool(&self, package: &str) -> bool {
        self.tools.contains(package)
    }

    /// The same exact location grants owner construction and task-start authority.
    pub fn is_composition_root(&self, path: &str, owner: &str, method: &str) -> bool {
        self.composition_roots
            .iter()
            .any(|root| root.path == path && root.owner == owner && root.method == method)
    }

    /// The database's capability, whose homes alone hold raw SQLite handles
    /// and SQL on the database's internal tables.
    pub fn database_capability(&self) -> Option<&Capability> {
        let database = self.database.as_ref()?;
        self.capabilities.iter().find(|capability| capability.name == database.capability)
    }

    /// The homes of the database's capability.
    pub fn database_homes(&self) -> &[String] {
        self.database_capability().map_or(&[], |capability| capability.homes.as_slice())
    }

    /// Whether the callable `callable` of `path` is listed as taking a raw
    /// database handle.
    pub fn allows_raw_handle_parameter(&self, path: &str, callable: &str) -> bool {
        self.database.as_ref().is_some_and(|database| {
            database
                .raw_handle_parameters
                .iter()
                .any(|row| row.path == path && row.callable == callable)
        })
    }

    /// The Cargo features only tests turn on (`syntax::is_test_only`).
    pub fn test_support_features(&self) -> &[String] {
        &self.test_support_features
    }

    /// Every type the owner rules' rows name, by path, with the row it came
    /// from.
    pub fn owner_type_rows(&self) -> Vec<(&'static str, &str)> {
        let lists: [(&str, &BTreeSet<String>); 11] = [
            ("capability_types", &self.capability_types),
            ("shared_state_types", &self.shared_state_types),
            ("private_state_types", &self.private_state_types),
            ("shared_pointer_types", &self.shared_pointer_types),
            ("capability_traits", &self.capability_traits),
            ("construction_only_capability_types", &self.construction_only_capability_types),
            ("non_owner_types", &self.non_owner_types),
            ("borrowed_facade_types", &self.borrowed_facade_types),
            ("task_types", &self.task_types),
            ("internal_dependency_types", &self.internal_dependency_types),
            ("closed_session_types", &self.closed_session_types),
        ];
        let mut named = lists
            .into_iter()
            .flat_map(|(row, names)| names.iter().map(move |name| (row, name.as_str())))
            .collect::<Vec<_>>();
        for (handle, owner) in &self.handles {
            named.push(("handles", handle.as_str()));
            named.push(("handles", owner.as_str()));
        }
        for factory in &self.capability_factories {
            named.push(("capability_factories", factory.product.as_str()));
        }
        named
    }

    /// Every type the policy names, with the row it came from: the owner
    /// rules' and the read rule's rows and the domains' owners by path
    /// ([`Policy::owner_type_rows`], [`ReadShapes`]), the others by name.
    pub fn named_types(&self) -> Vec<(&'static str, &str)> {
        let mut named = self.owner_type_rows();
        for root in &self.composition_roots {
            if root.owner != "<free>" {
                named.push(("composition_roots", root.owner.as_str()));
            }
        }
        for domain in self.database.iter().flat_map(|database| &database.domains) {
            named.push(("database.domains", domain.owner.as_str()));
        }
        for write in self.database.iter().flat_map(|database| &database.write_types) {
            named.push(("database.write_types", write.as_str()));
        }
        if let Some(read) = &self.read_shapes {
            let lists = [
                ("read_shapes.read_types", &read.read_types),
                ("read_shapes.live_query_types", &read.live_query_types),
                ("read_shapes.request_query_types", &read.request_query_types),
                ("read_shapes.value_stream_types", &read.value_stream_types),
                ("read_shapes.kept_state_types", &read.kept_state_types),
            ];
            for (row, names) in lists {
                named.extend(names.iter().map(|name| (row, name.as_str())));
            }
        }
        for factory in &self.capability_factories {
            named.push(("capability_factories", factory.owner.as_str()));
        }
        named
    }

    /// A policy from its text, for tests to name the rows they exercise.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fixture(text: &str) -> Policy {
        toml::from_str(text).expect("parse fixture policy")
    }
}

/// The two lists of a rule's rows: the violations the workspace has today,
/// and the findings that are not violations.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ListKind {
    /// Violations the workspace has today.
    Frozen,
    /// Findings that are not violations.
    Allowed,
}

impl ListKind {
    /// The list's key in the policy file.
    pub fn name(self) -> &'static str {
        match self {
            ListKind::Frozen => "frozen",
            ListKind::Allowed => "allowed",
        }
    }
}

/// Whether a home, a source file or a directory (ending in `/`) of them,
/// holds the file at `path`.
pub fn home_holds(home: &str, path: &str) -> bool {
    if home.ends_with('/') {
        path.starts_with(home)
    } else {
        path == home
    }
}

/// Where a capability is used directly, as findings name it.
pub fn describe_homes(homes: &[String]) -> String {
    if homes.is_empty() {
        "no file yet".to_string()
    } else {
        homes.join(", ")
    }
}

/// `[composition_roots]`: each source file's callables, `Type::method` or a
/// free function's or static's name.
fn composition_roots<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<CompositionRoot>, D::Error> {
    let mut roots = Vec::new();
    for (path, callables) in BTreeMap::<String, Vec<String>>::deserialize(deserializer)? {
        for callable in callables {
            let (owner, method) = match split_callable(&callable) {
                Some((owner, method)) => (owner.to_string(), method.to_string()),
                None if is_identifier(&callable) => ("<free>".to_string(), callable),
                None => {
                    return Err(D::Error::custom(format!(
                        "composition root {callable:?} in {path} is neither `Type::method` nor a function name"
                    )));
                }
            };
            roots.push(CompositionRoot { path: path.clone(), owner, method });
        }
    }
    Ok(roots)
}

/// `[capability_factories]`: each source file's `"Type::method" = "Product"`.
fn capability_factories<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<CapabilityFactory>, D::Error> {
    let mut factories = Vec::new();
    for (path, methods) in BTreeMap::<String, BTreeMap<String, String>>::deserialize(deserializer)?
    {
        for (callable, product) in methods {
            let Some((owner, method)) = split_callable(&callable) else {
                return Err(D::Error::custom(format!(
                    "capability factory {callable:?} in {path} is not `Type::method`"
                )));
            };
            factories.push(CapabilityFactory {
                path: path.clone(),
                owner: owner.to_string(),
                method: method.to_string(),
                product,
            });
        }
    }
    Ok(factories)
}

/// The type a path names, by its own name: the path's last segment.
pub fn type_name(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
}

/// `Type::method`, both parts identifiers.
fn split_callable(callable: &str) -> Option<(&str, &str)> {
    callable
        .split_once("::")
        .filter(|(owner, method)| is_identifier(owner) && is_identifier(method))
}

/// Whether `name` is a Rust identifier.
fn is_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| character.is_alphanumeric() || character == '_')
}

/// Rust paths written as text, `std::fs`, split into their segments.
fn rust_paths<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Vec<String>>, D::Error> {
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|path| {
            let segments = path.split("::").map(str::to_string).collect::<Vec<_>>();
            if segments.iter().any(|segment| !is_identifier(segment)) {
                return Err(D::Error::custom(format!("{path:?} is not a Rust path")));
            }
            Ok(segments)
        })
        .collect()
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
