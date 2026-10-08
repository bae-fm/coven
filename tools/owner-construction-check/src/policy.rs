//! The shape of the policy file (`owner_policy.rs`, §20.2): every name the
//! rules read about this workspace — the crate order, where each capability
//! lives, the construction and task-start roots, and the types that
//! are capabilities, owners, tasks or plain values.
//!
//! Guard tests require listed files, types and callables to exist.

/// `(source file, type, method)`: a callable allowed to construct owners or
/// start tasks (§20.2), including owner constructors and feature-gated fixtures.
/// `<free>` in the type position names a free function such as restore bootstrap.
pub(crate) type CompositionRoot = (&'static str, &'static str, &'static str);

/// `(source file, owner, receiver method, capability)`: an owner may derive
/// this capability from the one it was given, such as a directory naming one
/// of its files. This grants no authority to construct unrelated capabilities.
pub(crate) type CapabilityFactory = (&'static str, &'static str, &'static str, &'static str);

/// `(schema file, table macros)`: where coven's own tables are declared. Each
/// `$visit!(table, …)` inside one of the named `macro_rules!` declares a
/// table whose SQL only the database crate writes.
pub(crate) type DatabaseSchema = (&'static str, &'static [&'static str]);

pub(crate) struct Policy {
    /// §20.1: the workspace's crates in dependency order. A crate depends only
    /// on crates earlier in this list. Rows are added as crates land, in the
    /// spec's order.
    pub(crate) crate_order: &'static [&'static str],
    /// §20.1: pairs of crates in `crate_order` that never depend on each
    /// other in either direction.
    pub(crate) separated_crates: &'static [(&'static str, &'static str)],
    /// §20.2: each capability, the one place that uses it directly, and what
    /// using it directly looks like in source.
    pub(crate) capabilities: Capabilities,
    pub(crate) database_schema: Option<DatabaseSchema>,
    pub(crate) composition_roots: &'static [CompositionRoot],
    /// Runtime acquisition and task starts require an entry in the same list.
    pub(crate) task_starts: &'static [Gate],
    pub(crate) capability_factories: &'static [CapabilityFactory],
    /// Types that hold a capability. A type holding one of these, or holding
    /// an owner, is an owner.
    pub(crate) capability_types: &'static [&'static str],
    /// Capability interfaces. Every workspace implementation is construction-only,
    /// including generic implementations and feature-gated test support.
    pub(crate) capability_traits: &'static [&'static str],
    /// Raw capabilities fixed when the owner graph is built, such as the store
    /// directory, which have no capability trait. These and the implementations
    /// of `capability_traits` may be acquired only at composition roots, and
    /// accepted as parameters only by constructors and composition roots.
    pub(crate) construction_only_capability_types: &'static [&'static str],
    /// Values, configuration and proofs that name a capability in a field but
    /// do not own its lifetime.
    pub(crate) non_owner_types: &'static [&'static str],
    /// API namespaces that borrow an owner without becoming one.
    pub(crate) borrowed_facade_types: &'static [&'static str],
    /// Tasks (§20.2): values that live for one piece of work and are then
    /// dropped, such as one sync pass.
    pub(crate) task_types: &'static [&'static str],
    /// Dependencies an owner keeps to itself; returning one it retains leaks
    /// it. The raw SQLite handles are always included.
    pub(crate) internal_dependency_types: &'static [&'static str],
    /// Sessions that borrow internal dependencies for closed work. One
    /// declared at a crate root would expose those dependencies to every
    /// module of the crate.
    pub(crate) closed_session_types: &'static [&'static str],
}

/// §20.2's table: each capability and the one place that uses it directly.
pub(crate) struct Capabilities {
    pub(crate) network: Capability,
    pub(crate) cryptography: Capability,
    pub(crate) sqlite: Capability,
    pub(crate) keychain: Capability,
    pub(crate) time: Capability,
    pub(crate) ids: Capability,
    pub(crate) files: Capability,
}

impl Capabilities {
    pub(crate) fn all(&self) -> [&Capability; 7] {
        [
            &self.network,
            &self.cryptography,
            &self.sqlite,
            &self.keychain,
            &self.time,
            &self.ids,
            &self.files,
        ]
    }
}

pub(crate) struct Capability {
    pub(crate) name: &'static str,
    /// Path prefixes, relative to the workspace root, where the capability is
    /// used directly. A prefix ending in `/` names a directory.
    pub(crate) homes: &'static [&'static str],
    pub(crate) gates: &'static [Gate],
}

/// One way of using a capability directly: naming any of `crates`, writing a
/// path containing any of `path_patterns` as adjacent segments, or calling a
/// method named in `method_patterns`.
pub(crate) struct Gate {
    pub(crate) kind: &'static str,
    pub(crate) crates: &'static [&'static str],
    pub(crate) path_patterns: &'static [&'static [&'static str]],
    /// Method names whose receiver-call form (`x.name(...)`) is gated; `syn`
    /// does not surface a method call as a path.
    pub(crate) method_patterns: &'static [&'static str],
}

#[cfg(test)]
impl Capability {
    pub(crate) const NONE: Capability = Capability {
        name: "none",
        homes: &[],
        gates: &[],
    };
}

impl Policy {
    /// The same exact location grants owner construction and task-start authority.
    pub(crate) fn is_composition_root(&self, path: &str, owner: &str, method: &str) -> bool {
        self.composition_roots.contains(&(path, owner, method))
    }

    /// Every type name the policy holds, with the row it came from.
    pub(crate) fn named_types(&self) -> Vec<(&'static str, &'static str)> {
        let lists: [(&str, &[&str]); 8] = [
            ("capability_types", self.capability_types),
            ("capability_traits", self.capability_traits),
            (
                "construction_only_capability_types",
                self.construction_only_capability_types,
            ),
            ("non_owner_types", self.non_owner_types),
            ("borrowed_facade_types", self.borrowed_facade_types),
            ("task_types", self.task_types),
            ("internal_dependency_types", self.internal_dependency_types),
            ("closed_session_types", self.closed_session_types),
        ];
        let mut named = lists
            .into_iter()
            .flat_map(|(row, names)| names.iter().map(move |name| (row, *name)))
            .collect::<Vec<_>>();
        for (_, owner, _) in self.composition_roots {
            if *owner != "<free>" {
                named.push(("composition_roots", *owner));
            }
        }
        for (_, owner, _, product) in self.capability_factories {
            named.push(("capability_factories", owner));
            named.push(("capability_factories", product));
        }
        named
    }

    /// A policy naming nothing, for tests to fill in the rows they exercise.
    #[cfg(test)]
    pub(crate) const EMPTY: Policy = Policy {
        crate_order: &[],
        separated_crates: &[],
        capabilities: Capabilities {
            network: Capability::NONE,
            cryptography: Capability::NONE,
            sqlite: Capability::NONE,
            keychain: Capability::NONE,
            time: Capability::NONE,
            ids: Capability::NONE,
            files: Capability::NONE,
        },
        database_schema: None,
        composition_roots: &[],
        task_starts: &[],
        capability_factories: &[],
        capability_types: &[],
        capability_traits: &[],
        construction_only_capability_types: &[],
        non_owner_types: &[],
        borrowed_facade_types: &[],
        task_types: &[],
        internal_dependency_types: &[],
        closed_session_types: &[],
    };
}
