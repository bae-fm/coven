//! The policy file (Appendix F.3): what the checked workspace says about
//! itself. The workspace keeps it at its root as `owner-construction-check.toml`:
//! the rules it runs, its crates in dependency order, where each capability
//! lives, the construction and task-start roots, and the types that are
//! capabilities, handles, owners, tasks or plain values. The checker's source
//! names none of them; each workspace that runs it supplies its own.
//!
//! `policy_check.rs` holds every row against the workspace before any rule
//! runs, so a row naming something that moved fails instead of covering nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// The policy file's name, at the root of the workspace it describes.
pub(crate) const FILE_NAME: &str = "owner-construction-check.toml";

/// The groups of rules a policy selects. A workspace runs exactly the ones it
/// lists, so it can adopt them one at a time.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Rule {
    /// Appendix F.1: the crate order, and dependency versions set once in the
    /// workspace (`crate_dependencies.rs`).
    CrateDependencies,
    /// Appendix F.2: each capability used directly only in its homes, and
    /// task starts only at composition roots (`capability_boundaries.rs`).
    CapabilityBoundaries,
    /// Appendix F.2: raw SQLite handles and SQL on the database's internal
    /// tables stay in the database's homes, and each domain's tables are
    /// written only in its homes (`database_boundary.rs`).
    DatabaseBoundary,
    /// Appendix F.3: an owner never hands out what it holds, and only its
    /// owner hands out a handle (`owner_dependency_boundary.rs`).
    OwnerDependencies,
    /// Appendix F.2 and F.3: an owner's method never takes a
    /// construction-only capability, and no method but a constructor takes
    /// a handle (`retained_capability_parameters.rs`).
    RetainedCapabilityParameters,
    /// Appendix F.3: no struct built only to be destructured
    /// (`component_bundles.rs`).
    ComponentBundles,
    /// Appendix F.3: owners and capabilities are constructed only at
    /// composition roots (`capability_construction.rs`).
    CapabilityConstruction,
    /// Appendix F.5: visibility, file length and test layout (`conventions.rs`).
    Conventions,
}

impl Rule {
    /// Whether the rule reads the crates' code (`crate_code.rs`): every rule
    /// but the crate dependencies, which read manifests, and the conventions,
    /// which read every file as written.
    pub(crate) fn reads_crate_code(self) -> bool {
        !matches!(self, Rule::CrateDependencies | Rule::Conventions)
    }

    /// Whether the rule infers owners and matches types by name, so it needs
    /// every name it reasons about declared once (`unique_type_names.rs`).
    pub(crate) fn reads_owner_graph(self) -> bool {
        matches!(
            self,
            Rule::OwnerDependencies
                | Rule::RetainedCapabilityParameters
                | Rule::CapabilityConstruction
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    /// The rule groups this workspace runs.
    #[serde(default)]
    pub(crate) rules: BTreeSet<Rule>,
    /// Workspace members that answer only to the conventions, such as this
    /// checker. Every other member is a crate and answers to every rule.
    #[serde(default)]
    pub(crate) tools: BTreeSet<String>,
    /// The crate whose root re-exports the API and keeps every module private
    /// (Appendix F.5).
    #[serde(default)]
    pub(crate) api_crate: Option<String>,
    /// Appendix F.1: the workspace's crates in dependency order. A crate depends
    /// only on crates earlier in this list.
    #[serde(default)]
    pub(crate) crate_order: Vec<String>,
    /// Appendix F.1: pairs of crates in `crate_order` that never depend on each
    /// other in either direction.
    #[serde(default)]
    pub(crate) separated_crates: Vec<(String, String)>,
    /// Appendix F.2: each capability, the one place that uses it directly, and
    /// what using it directly looks like in source.
    #[serde(default)]
    pub(crate) capabilities: Vec<Capability>,
    #[serde(default)]
    pub(crate) database: Option<Database>,
    /// Every callable allowed to construct owners or start tasks (Appendix F.3),
    /// including owner constructors and feature-gated fixtures. The file lists
    /// them by source file: `Type::method`, or a free function's name.
    #[serde(default, deserialize_with = "composition_roots")]
    pub(crate) composition_roots: Vec<CompositionRoot>,
    /// Runtime acquisition and task starts require an entry in
    /// `composition_roots`.
    #[serde(default)]
    pub(crate) task_starts: Vec<Gate>,
    /// Receiver methods through which an owner may derive a capability from the
    /// one it was given, such as a directory naming one of its files. The file
    /// lists them by source file: `"Type::method" = "Product"`.
    #[serde(default, deserialize_with = "capability_factories")]
    pub(crate) capability_factories: Vec<CapabilityFactory>,
    /// Types that hold a capability. A type holding one of these, or holding
    /// an owner, is an owner.
    #[serde(default)]
    pub(crate) capability_types: BTreeSet<String>,
    /// Capability interfaces. Every workspace implementation is construction-only,
    /// including generic implementations and feature-gated test support.
    #[serde(default)]
    pub(crate) capability_traits: BTreeSet<String>,
    /// Handles (Appendix F.3), each with the type that makes it:
    /// `LiveQueryRequests = "LiveQuery"`. A handle is a capability its owner
    /// makes from its own state to hand out: only that owner (or the handle,
    /// cloning itself) makes one; that owner returns one, and so does an
    /// owner holding it, at any depth, passing up a fresh one, but nothing
    /// holding the handle itself; and a holder receives one only when it is
    /// built or as a free function's argument.
    #[serde(default)]
    pub(crate) handles: BTreeMap<String, String>,
    /// Raw capabilities fixed when the owner graph is built, which have no
    /// capability trait. These and the implementations of `capability_traits`
    /// may be acquired only at composition roots, and accepted as parameters
    /// only by constructors and composition roots.
    #[serde(default)]
    pub(crate) construction_only_capability_types: BTreeSet<String>,
    /// Values, configuration and proofs that name a capability in a field but
    /// do not own its lifetime.
    #[serde(default)]
    pub(crate) non_owner_types: BTreeSet<String>,
    /// API namespaces that borrow an owner without becoming one.
    #[serde(default)]
    pub(crate) borrowed_facade_types: BTreeSet<String>,
    /// Tasks (Appendix F.3): values that live for one piece of work and are
    /// then dropped.
    #[serde(default)]
    pub(crate) task_types: BTreeSet<String>,
    /// Dependencies an owner keeps to itself; returning one it retains leaks
    /// it. The raw SQLite handles are always included.
    #[serde(default)]
    pub(crate) internal_dependency_types: BTreeSet<String>,
    /// Sessions that borrow internal dependencies for closed work. One
    /// declared at a crate root would expose those dependencies to every
    /// module of the crate.
    #[serde(default)]
    pub(crate) closed_session_types: BTreeSet<String>,
    /// Generic types that hold no value of their type arguments, such as a
    /// router typed by the state it will later be given: `Router<AppState>`
    /// holds no `AppState`. A type argument of one is no value the type holds,
    /// so returning one constructs none and a field holding one retains none.
    #[serde(default)]
    pub(crate) phantom_type_arguments: BTreeSet<String>,
    /// Dependencies whose library, as source code names it, is not their
    /// package's name with `-` as `_`: `"coreaudio-rs" = "coreaudio"`. The
    /// checker reads the workspace's manifests, not its dependencies', so a
    /// library's own name reaches it only here.
    #[serde(default)]
    pub(crate) library_names: BTreeMap<String, String>,
    /// Names the type rows use for types that dependencies outside the
    /// workspace declare, such as a runtime handle or a lock. The rules match
    /// types by name, so each must be named by a crate source and declared by
    /// none.
    #[serde(default)]
    pub(crate) external_types: BTreeSet<String>,
}

/// `(source file, type, method)`: a callable allowed to construct owners or
/// start tasks. A free function has `<free>` in the type position.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CompositionRoot {
    pub(crate) path: String,
    pub(crate) owner: String,
    pub(crate) method: String,
}

/// `(source file, owner, receiver method, capability)`: an owner may derive
/// this capability from the one it was given. This grants no authority to
/// construct unrelated capabilities.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CapabilityFactory {
    pub(crate) path: String,
    pub(crate) owner: String,
    pub(crate) method: String,
    pub(crate) product: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Capability {
    pub(crate) name: String,
    /// Path prefixes, relative to the workspace root, where the capability is
    /// used directly. A prefix ending in `/` names a directory.
    #[serde(default)]
    pub(crate) homes: Vec<String>,
    #[serde(default)]
    pub(crate) gates: Vec<Gate>,
}

/// One way of using a capability directly: naming any of `crates`, writing a
/// path containing any of `paths` as adjacent segments, or calling a method
/// named in `methods`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Gate {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) crates: Vec<String>,
    /// Written as Rust paths, `std::fs`.
    #[serde(default, rename = "paths", deserialize_with = "rust_paths")]
    pub(crate) path_patterns: Vec<Vec<String>>,
    /// Method names whose receiver-call form (`x.name(...)`) is gated; `syn`
    /// does not surface a method call as a path.
    #[serde(default, rename = "methods")]
    pub(crate) method_patterns: Vec<String>,
}

/// The SQLite capability and the database's own tables: outside the named
/// capability's homes, nothing names a raw rusqlite handle or writes SQL on
/// one of the schema's tables.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Database {
    /// The capability, in `capabilities`, whose homes hold the raw handles.
    pub(crate) capability: String,
    /// The schema module's root file. The module is that file and every
    /// module beneath it, wherever `#[path]` puts its file. Each
    /// `$visit!(table, …)` inside one of `table_macros`, top-level
    /// `macro_rules!` in that module, declares a table whose SQL only the
    /// database's homes write.
    #[serde(default)]
    pub(crate) schema: Option<String>,
    #[serde(default)]
    pub(crate) table_macros: Vec<String>,
    /// A crate whose `rusqlite` re-export other crates may import: that path
    /// names the crate, not SQLite.
    #[serde(default)]
    pub(crate) rusqlite_reexport: Option<String>,
    /// The tables, grouped by the part of the workspace that writes them.
    #[serde(default)]
    pub(crate) domains: Vec<Domain>,
}

/// A domain: tables that only its homes write. SQL elsewhere may read them;
/// writing one — `INSERT`, `UPDATE`, `DELETE` or `REPLACE`, in a statement or
/// in a trigger's body — is the domain's own work (`sql_writes.rs`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Domain {
    pub(crate) name: String,
    /// Path prefixes, relative to the workspace root, where SQL writes the
    /// domain's tables. A prefix ending in `/` names a directory.
    pub(crate) homes: Vec<String>,
    /// Table names, as SQL writes them; they compare without case.
    pub(crate) tables: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PolicyError {
    #[error("read {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

impl Policy {
    /// The policy at the root of the workspace at `root`.
    pub(crate) fn read(root: &Path) -> Result<Policy, PolicyError> {
        let path = root.join(FILE_NAME);
        let text = std::fs::read_to_string(&path)
            .map_err(|source| PolicyError::Read { path: path.clone(), source })?;
        toml::from_str(&text).map_err(|source| PolicyError::Parse { path, source })
    }

    pub(crate) fn selects(&self, rule: Rule) -> bool {
        self.rules.contains(&rule)
    }

    /// Whether a workspace member answers only to the conventions.
    pub(crate) fn is_tool(&self, package: &str) -> bool {
        self.tools.contains(package)
    }

    /// The same exact location grants owner construction and task-start authority.
    pub(crate) fn is_composition_root(&self, path: &str, owner: &str, method: &str) -> bool {
        self.composition_roots
            .iter()
            .any(|root| root.path == path && root.owner == owner && root.method == method)
    }

    /// The homes of the database's capability: where raw SQLite handles and
    /// SQL on the database's internal tables are allowed.
    pub(crate) fn database_homes(&self) -> &[String] {
        self.database
            .as_ref()
            .and_then(|database| {
                self.capabilities.iter().find(|capability| capability.name == database.capability)
            })
            .map_or(&[], |capability| capability.homes.as_slice())
    }

    /// The types whose holder is an owner: the capability types and the
    /// handles.
    pub(crate) fn held_capabilities(&self) -> BTreeSet<String> {
        self.capability_types.iter().chain(self.handles.keys()).cloned().collect()
    }

    /// Whether `handle` is a handle and `maker` may make and return it: its
    /// owner, or the handle itself, as a clone does.
    pub(crate) fn makes_handle(&self, maker: &str, handle: &str) -> bool {
        self.handles.get(handle).is_some_and(|owner| owner == maker || handle == maker)
    }

    /// Every type name the policy holds, with the row it came from.
    pub(crate) fn named_types(&self) -> Vec<(&'static str, &str)> {
        let lists: [(&str, &BTreeSet<String>); 9] = [
            ("capability_types", &self.capability_types),
            ("capability_traits", &self.capability_traits),
            ("construction_only_capability_types", &self.construction_only_capability_types),
            ("non_owner_types", &self.non_owner_types),
            ("borrowed_facade_types", &self.borrowed_facade_types),
            ("task_types", &self.task_types),
            ("internal_dependency_types", &self.internal_dependency_types),
            ("closed_session_types", &self.closed_session_types),
            ("phantom_type_arguments", &self.phantom_type_arguments),
        ];
        let mut named = lists
            .into_iter()
            .flat_map(|(row, names)| names.iter().map(move |name| (row, name.as_str())))
            .collect::<Vec<_>>();
        for root in &self.composition_roots {
            if root.owner != "<free>" {
                named.push(("composition_roots", root.owner.as_str()));
            }
        }
        for (handle, owner) in &self.handles {
            named.push(("handles", handle.as_str()));
            named.push(("handles", owner.as_str()));
        }
        for factory in &self.capability_factories {
            named.push(("capability_factories", factory.owner.as_str()));
            named.push(("capability_factories", factory.product.as_str()));
        }
        named
    }

    /// A policy from its text, for tests to name the rows they exercise.
    #[cfg(test)]
    pub(crate) fn fixture(text: &str) -> Policy {
        toml::from_str(text).expect("parse fixture policy")
    }

    /// The fixture policy the rule tests share: every rule, and the
    /// capability table and task starts of a workspace of `app-*` crates.
    #[cfg(test)]
    pub(crate) fn gated_fixture() -> Policy {
        Policy::fixture(include_str!("policy_fixture.toml"))
    }
}

/// Where a capability is used directly, as findings name it.
pub(crate) fn describe_homes(homes: &[String]) -> String {
    if homes.is_empty() {
        "no file yet".to_string()
    } else {
        homes.join(", ")
    }
}

/// `[composition_roots]`: each source file's callables, `Type::method` or a
/// free function's name.
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
                    )))
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

/// `Type::method`, both parts identifiers.
fn split_callable(callable: &str) -> Option<(&str, &str)> {
    callable
        .split_once("::")
        .filter(|(owner, method)| is_identifier(owner) && is_identifier(method))
}

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
#[path = "policy_tests.rs"]
mod tests;
