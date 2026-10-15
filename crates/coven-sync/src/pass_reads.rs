//! Discovery misses and listings shared within one completed observation.

use coven_foundation::id_source::DeviceId;
use coven_storage::StoredObject;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct PassReads(Arc<Mutex<Reads>>);

#[derive(Default)]
struct Reads {
    scopes: usize,
    positions: Option<crate::posted_positions::Observation>,
    writers: Option<BTreeSet<DeviceId>>,
    misses: BTreeSet<(coven_database::LogKind, DeviceId, u64)>,
    /// Retired devices whose entry and key-copy logs reached terminal misses in
    /// a completed discovery, by the storage sample taken before it (§6).
    drains: BTreeMap<DeviceId, coven_format::value::StorageTime>,
    /// This pass's first phase: whether it completed with sending authority.
    membership: Option<bool>,
    /// Audience snapshot listings shared while one return observation lasts.
    snapshot_listings: Option<BTreeMap<String, Vec<StoredObject>>>,
}

impl PassReads {
    /// Nested snapshot operations share their caller's pass. The last scope
    /// discards misses and writer discovery, including on error or cancellation;
    /// another pass observes storage afresh. Object facts live in the database.
    pub(crate) fn enter(&self) -> ReadScope {
        self.0.lock().expect("pass reads lock poisoned").scopes += 1;
        ReadScope(self.clone())
    }

    pub(crate) fn positions(&self) -> Option<crate::posted_positions::Observation> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .positions
            .clone()
    }

    pub(crate) fn keep_positions(&self, positions: crate::posted_positions::Observation) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads.positions = Some(positions);
        }
    }

    /// Start a new discovery observation: list writers and probe every log again.
    pub(crate) fn reset_discovery(&self) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        reads.writers = None;
        reads.misses.clear();
    }

    /// Probe one kind of log again, including writers registered since the
    /// listing, while keeping the other kind's misses from this observation.
    pub(crate) fn reset_discovery_of(&self, kind: coven_database::LogKind) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        reads.writers = None;
        reads.misses.retain(|(missed, _, _)| *missed != kind);
    }

    /// This device's own publications extend only its own log.
    pub(crate) fn forget_misses(&self, kind: coven_database::LogKind, writer: DeviceId) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        reads
            .misses
            .retain(|(missed, device, _)| (*missed, *device) != (kind, writer));
    }

    pub(crate) fn writers(&self) -> Option<BTreeSet<DeviceId>> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .writers
            .clone()
    }

    pub(crate) fn keep_writers(&self, writers: BTreeSet<DeviceId>) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads.writers = Some(writers);
        }
    }

    pub(crate) fn missed(
        &self,
        kind: coven_database::LogKind,
        writer: DeviceId,
        number: u64,
    ) -> bool {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .misses
            .contains(&(kind, writer, number))
    }

    /// Whether this observation reached a terminal miss in `writer`'s log.
    pub(crate) fn reached_miss(&self, kind: coven_database::LogKind, writer: DeviceId) -> bool {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .misses
            .iter()
            .any(|(missed, device, _)| (*missed, *device) == (kind, writer))
    }

    /// Replace the drain halves of the latest completed membership discovery:
    /// each device's entry and key-copy misses followed this storage sample.
    pub(crate) fn keep_drains(&self, drains: BTreeMap<DeviceId, coven_format::value::StorageTime>) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads.drains = drains;
        }
    }

    pub(crate) fn drains(&self) -> BTreeMap<DeviceId, coven_format::value::StorageTime> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .drains
            .clone()
    }

    /// Record the outcome of this pass's read-only first phase.
    pub(crate) fn keep_membership(&self, complete: bool) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads.membership = Some(complete);
        }
    }

    /// This pass's first phase, if one ran: operations reuse it (SP step 5).
    pub(crate) fn membership(&self) -> Option<bool> {
        self.0.lock().expect("pass reads lock poisoned").membership
    }

    pub(crate) fn keep_miss(&self, kind: coven_database::LogKind, writer: DeviceId, number: u64) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads.misses.insert((kind, writer, number));
        }
    }

    /// A return's snapshot observation also serves the reload it starts,
    /// without listing again (§15). Sharing ends when the guard drops.
    pub(crate) fn share_snapshot_listings(&self) -> SharedListings {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .snapshot_listings
            .get_or_insert_with(BTreeMap::new);
        SharedListings(self.clone())
    }

    pub(crate) fn snapshot_listing(&self, prefix: &str) -> Option<Vec<StoredObject>> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .snapshot_listings
            .as_ref()
            .and_then(|listings| listings.get(prefix).cloned())
    }

    pub(crate) fn keep_snapshot_listing(&self, prefix: &str, objects: &[StoredObject]) {
        if let Some(listings) = &mut self
            .0
            .lock()
            .expect("pass reads lock poisoned")
            .snapshot_listings
        {
            listings.insert(prefix.to_owned(), objects.to_vec());
        }
    }
}

pub(crate) struct ReadScope(PassReads);

impl Drop for ReadScope {
    fn drop(&mut self) {
        let mut reads = self.0 .0.lock().expect("pass reads lock poisoned");
        reads.scopes -= 1;
        if reads.scopes == 0 {
            reads.positions = None;
            reads.writers = None;
            reads.misses.clear();
            reads.drains.clear();
            reads.membership = None;
        }
    }
}

pub(crate) struct SharedListings(PassReads);

impl Drop for SharedListings {
    fn drop(&mut self) {
        self.0
             .0
            .lock()
            .expect("pass reads lock poisoned")
            .snapshot_listings = None;
    }
}
