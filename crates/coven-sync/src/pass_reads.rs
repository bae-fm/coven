//! Discovery misses and listings shared within one completed observation.

use coven_foundation::id_source::{DeviceId, FileId};
use coven_storage::StoredObject;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

pub(crate) type FileReferences = BTreeSet<(DeviceId, FileId)>;

#[derive(Clone, Default)]
pub(crate) struct PassReads(Arc<Mutex<Reads>>);

#[derive(Default)]
struct Reads {
    scopes: usize,
    positions: Option<crate::posted_positions::Observation>,
    writers: Option<BTreeSet<DeviceId>>,
    misses: BTreeSet<(coven_database::LogKind, DeviceId, u64)>,
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
