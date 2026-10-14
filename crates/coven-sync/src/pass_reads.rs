//! Reuse immutable object metadata and checked file references within one pass.

use coven_foundation::id_source::{DeviceId, FileId};
use coven_storage::{ObjectPath, StoredObject};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

pub(crate) type FileReferences = BTreeSet<(DeviceId, FileId)>;

#[derive(Clone, Default)]
pub(crate) struct PassReads(Arc<Mutex<Reads>>);

#[derive(Default)]
struct Reads {
    scopes: usize,
    writers: Option<BTreeSet<DeviceId>>,
    misses: BTreeSet<(coven_database::LogKind, DeviceId, u64)>,
    metadata: BTreeMap<ObjectPath, (StoredObject, Vec<u8>)>,
    files: BTreeMap<ObjectPath, (StoredObject, Option<FileReferences>)>,
}

impl PassReads {
    /// Nested snapshot operations share their caller's pass. The last scope
    /// discards everything, including on error or cancellation; another pass
    /// observes storage afresh. Only bounded metadata and references are kept.
    pub(crate) fn enter(&self) -> ReadScope {
        self.0.lock().expect("pass reads lock poisoned").scopes += 1;
        ReadScope(self.clone())
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

    pub(crate) fn metadata(&self, object: &StoredObject) -> Option<Vec<u8>> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .metadata
            .get(&object.path)
            .filter(|(stored, _)| stored == object)
            .map(|(_, bytes)| bytes.clone())
    }

    pub(crate) fn keep_metadata(&self, object: &StoredObject, bytes: Vec<u8>) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads
                .metadata
                .insert(object.path.clone(), (object.clone(), bytes));
        }
    }

    /// An outer `None` means unread; an inner `None` means a part or schema
    /// cannot be read, so the object cannot prove file absence.
    pub(crate) fn files(&self, object: &StoredObject) -> Option<Option<FileReferences>> {
        self.0
            .lock()
            .expect("pass reads lock poisoned")
            .files
            .get(&object.path)
            .filter(|(stored, _)| stored == object)
            .map(|(_, files)| files.clone())
    }

    pub(crate) fn keep_files(&self, object: &StoredObject, files: Option<FileReferences>) {
        let mut reads = self.0.lock().expect("pass reads lock poisoned");
        if reads.scopes > 0 {
            reads
                .files
                .insert(object.path.clone(), (object.clone(), files));
        }
    }
}

pub(crate) struct ReadScope(PassReads);

impl Drop for ReadScope {
    fn drop(&mut self) {
        let mut reads = self.0 .0.lock().expect("pass reads lock poisoned");
        reads.scopes -= 1;
        if reads.scopes == 0 {
            reads.writers = None;
            reads.misses.clear();
            reads.metadata.clear();
            reads.files.clear();
        }
    }
}
