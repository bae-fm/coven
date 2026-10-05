//! The shape of the policy file (`owner_policy.rs`, §21.2): every name the
//! rules read about this workspace — the crate order, where each capability
//! lives, the composition roots, the lifetime authorities, and the types that
//! are capabilities, owners, tasks or plain values.
//!
//! Rows whose subject has not been written yet are absent, not anticipated:
//! each crate's PR adds its own. The capability homes are the one exception,
//! named ahead as the paths §21.2 assigns them; the guard tests skip a home
//! whose crate is not in the workspace yet and fail on one that names a missing
//! path inside a crate that is.
//!
//! Rows are tuples rather than structs because most lists start empty, and a
//! struct no production row constructs would be dead code.

/// `(source file, type, method)`: a method that builds owners (§21.2), such
/// as the builder's `open` or a test fixture that builds the same graph.
pub(crate) type CompositionRoot = (&'static str, &'static str, &'static str);

/// `(service, authority)`: the one owner that may build a service it replaces
/// while the store is open, such as the sync owner starting its sync loop.
pub(crate) type LifetimeAuthority = (&'static str, &'static str);

/// `(trait or owner, methods)`: raw provider operations an owner never offers.
pub(crate) type RawProviderOperations = (&'static str, &'static [&'static str]);

/// `(returned type, sources)`: a value an owner derives from one of `sources`
/// and so never hands to a caller when it is, or retains, one of them.
pub(crate) type DerivedService = (&'static str, &'static [&'static str]);

/// `(schema file, table macros)`: where coven's own tables are declared. Each
/// `$visit!(table, …)` inside one of the named `macro_rules!` declares a
/// table whose SQL only the database crate writes.
pub(crate) type DatabaseSchema = (&'static str, &'static [&'static str]);

pub(crate) struct Policy {
    /// §21.1: the workspace's crates in dependency order. A crate depends only
    /// on crates earlier in this list. Rows are added as crates land, in the
    /// spec's order.
    pub(crate) crate_order: &'static [&'static str],
    /// §21.1: pairs of crates in `crate_order` that never depend on each
    /// other in either direction.
    pub(crate) separated_crates: &'static [(&'static str, &'static str)],
    /// §21.2: each capability, the one place that uses it directly, and what
    /// using it directly looks like in source.
    pub(crate) capabilities: Capabilities,
    pub(crate) database_schema: Option<DatabaseSchema>,
    pub(crate) composition_roots: &'static [CompositionRoot],
    pub(crate) lifetime_authorities: &'static [LifetimeAuthority],
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
    /// The roots of the retained owner graph: what the builder's `open`
    /// returns, and what it holds.
    pub(crate) root_owner_types: &'static [&'static str],
    /// Tasks (§21.2): values that live for one piece of work and are then
    /// dropped, such as one sync pass.
    pub(crate) task_types: &'static [&'static str],
    /// Dependencies an owner keeps to itself; returning one it retains leaks
    /// it. The raw SQLite handles are always included.
    pub(crate) internal_dependency_types: &'static [&'static str],
    /// Dependencies no method returns at all. The raw SQLite handles are
    /// always included.
    pub(crate) always_forbidden_returns: &'static [&'static str],
    /// Sessions that borrow internal dependencies for closed work. One
    /// declared at a crate root would expose those dependencies to every
    /// module of the crate.
    pub(crate) closed_session_types: &'static [&'static str],
    /// Types beyond `capability_types` whose holder counts as a service owner
    /// for the owner dependency boundary's field and return rules.
    pub(crate) field_capability_types: &'static [&'static str],
    pub(crate) raw_provider_operations: &'static [RawProviderOperations],
    pub(crate) derived_services: &'static [DerivedService],
    /// Capabilities a service owner never returns, even one it builds fresh,
    /// and never accepts while it retains one.
    pub(crate) unexported_capability_types: &'static [&'static str],
    /// Of `unexported_capability_types`, the ones that are a task's product
    /// and so may be returned.
    pub(crate) exportable_capability_outputs: &'static [&'static str],
}

/// §21.2's table: each capability and the one place that uses it directly.
pub(crate) struct Capabilities {
    pub(crate) network: Capability,
    pub(crate) cryptography: Capability,
    pub(crate) sqlite: Capability,
    pub(crate) keychain: Capability,
    pub(crate) time: Capability,
    pub(crate) ids: Capability,
    pub(crate) files: Capability,
    /// Runtimes, threads and spawned futures: work that outlives the call
    /// that starts it. Each long-lived task has one lifetime authority, the
    /// only owner that may start it, so these are confined to the files of
    /// those authorities.
    pub(crate) runtimes: Capability,
}

impl Capabilities {
    pub(crate) fn all(&self) -> [&Capability; 8] {
        [
            &self.network,
            &self.cryptography,
            &self.sqlite,
            &self.keychain,
            &self.time,
            &self.ids,
            &self.files,
            &self.runtimes,
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

#[cfg(test)]
impl Policy {
    /// A policy naming nothing, for tests to fill in the rows they exercise.
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
            runtimes: Capability::NONE,
        },
        database_schema: None,
        composition_roots: &[],
        lifetime_authorities: &[],
        capability_types: &[],
        capability_traits: &[],
        construction_only_capability_types: &[],
        non_owner_types: &[],
        borrowed_facade_types: &[],
        root_owner_types: &[],
        task_types: &[],
        internal_dependency_types: &[],
        always_forbidden_returns: &[],
        closed_session_types: &[],
        field_capability_types: &[],
        raw_provider_operations: &[],
        derived_services: &[],
        unexported_capability_types: &[],
        exportable_capability_outputs: &[],
    };
}
