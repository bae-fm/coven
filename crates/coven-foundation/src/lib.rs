//! Foundation: the injectable primitives and value types every other Coven
//! crate is allowed to name.
//!
//! Nothing here knows about protocol state, storage providers, replication, or
//! the host API — these are clocks, identifier sources, staged file writes,
//! store directory layout, content hashes, changeset values, and configuration.
//! The dependency direction of the workspace bottoms out here, so this crate
//! has no Coven dependencies of its own.
//!
//! Modules are public because the crates above address them by module path
//! (`coven_foundation::clock::Clock`), the same paths they used when these
//! modules lived inside `coven`. Items that stay inside this crate remain
//! `pub(crate)` or narrower.

pub mod atomic_file;
pub mod blocking;
pub mod changeset;
pub mod clock;
pub mod code_envelope;
pub mod config;
pub mod id_provider;
pub mod local_file;
pub mod object_hash;
pub mod store_dir;
