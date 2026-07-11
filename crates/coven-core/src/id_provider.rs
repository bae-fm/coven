//! Identifier source, injected so tests get a deterministic — but still
//! unique — id sequence.
//!
//! Production wires [`UuidProvider`] (`Uuid::new_v4().to_string()`); tests
//! construct a [`SequentialIdProvider`] and pass it to the unit under test.
//!
//! Each `new_id()` call yields a fresh, distinct id. Per-entity uniqueness is
//! preserved: a loop minting one id per row keeps producing distinct ids under
//! both the production and the test provider.

use std::sync::Arc;
use uuid::Uuid;

/// Identifier source. Yields a fresh unique id per call.
pub trait IdProvider: Send + Sync {
    /// A fresh unique identifier as a string (matches every call site, which
    /// does `Uuid::new_v4().to_string()` today).
    fn new_id(&self) -> String;
}

/// Shared handle to an id provider. Held by `Clone` types that need to share
/// one id source, so they clone the handle, not the implementation.
pub type IdRef = Arc<dyn IdProvider>;

/// Production provider: random v4 UUIDs.
pub struct UuidProvider;

impl IdProvider for UuidProvider {
    fn new_id(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

// The deterministic id fake is exposed to downstream crates' tests via the
// `test-utils` feature, so any crate that consumes `IdProvider` tests against
// the same fake instead of mirroring it.
#[cfg(any(test, feature = "test-utils"))]
pub use fakes::SequentialIdProvider;

#[cfg(any(test, feature = "test-utils"))]
mod fakes {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Deterministic but unique: `"{prefix}-0"`, `"{prefix}-1"`, ... Preserves
    /// the per-entity uniqueness invariant while being reproducible across runs.
    pub struct SequentialIdProvider {
        prefix: String,
        next: AtomicU64,
    }

    impl SequentialIdProvider {
        pub fn new(prefix: &str) -> Self {
            Self {
                prefix: prefix.to_string(),
                next: AtomicU64::new(0),
            }
        }
    }

    impl IdProvider for SequentialIdProvider {
        fn new_id(&self) -> String {
            let n = self.next.fetch_add(1, Ordering::SeqCst);
            format!("{}-{}", self.prefix, n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_ids_are_deterministic_and_unique() {
        let ids = SequentialIdProvider::new("id");
        assert_eq!(ids.new_id(), "id-0");
        assert_eq!(ids.new_id(), "id-1");
        assert_eq!(ids.new_id(), "id-2");
    }

    #[test]
    fn provider_is_usable_behind_the_shared_handle() {
        let ids: IdRef = Arc::new(SequentialIdProvider::new("row"));
        assert_eq!(ids.new_id(), "row-0");
        assert_ne!(ids.new_id(), ids.new_id());
    }
}
