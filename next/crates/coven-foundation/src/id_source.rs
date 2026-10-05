//! New ids, including each install's 64-bit device id (§10).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A store's identity, independent of its name and location on disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreId(
    /// The store's UUID.
    pub Uuid,
);

impl std::fmt::Display for StoreId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// One install of the app, by its 64-bit device id (§10).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(
    /// The install's 64-bit id.
    pub u64,
);

/// The source of new ids, supplied when creating or opening a store.
pub trait IdSource: Send + Sync {
    /// A fresh UUID. Distinct calls must yield distinct ids.
    fn new_id(&self) -> Uuid;

    /// A fresh device id derived from this source, without another randomness
    /// source. Every install and restored copy receives a new device id (§10).
    ///
    /// Uses 62 payload bits from the UUID's last eight bytes and two from its
    /// eighth byte: all 64 are random in UUIDv4 and UUIDv7. The version,
    /// variant and UUIDv7 timestamp contribute no bits.
    fn new_device_id(&self) -> DeviceId {
        let (high, low) = self.new_id().as_u64_pair();
        DeviceId((low & 0x3fff_ffff_ffff_ffff) | ((high & 3) << 62))
    }
}

/// A shared source of new ids.
pub type IdSourceRef = Arc<dyn IdSource>;

/// Random UUIDv4 ids, independent of the clock (§20.1).
pub struct UuidIds;

impl IdSource for UuidIds {
    fn new_id(&self) -> Uuid {
        Uuid::new_v4()
    }
}

/// A deterministic sequence of UUIDs and device ids, starting at one.
///
/// One source shares its sequence across both methods. It panics on exhaustion
/// instead of wrapping and reusing an id. Separate sources repeat the sequence.
#[cfg(feature = "test-utils")]
pub struct SequentialIds(std::sync::atomic::AtomicU64);

#[cfg(feature = "test-utils")]
impl SequentialIds {
    /// A sequence whose first UUID's payload and first device id are one.
    pub fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(1))
    }
}

#[cfg(feature = "test-utils")]
impl IdSource for SequentialIds {
    fn new_id(&self) -> Uuid {
        use std::sync::atomic::Ordering;
        let n = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("sequential id source exhausted");
        // Preserve all 64 sequence bits in the payload positions used above.
        Uuid::from_u64_pair(
            0x7000 | (n >> 62),
            0x8000_0000_0000_0000 | (n & 0x3fff_ffff_ffff_ffff),
        )
    }
}

#[cfg(test)]
#[path = "id_source_tests.rs"]
mod tests;
