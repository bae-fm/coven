use super::Object;
use crate::ObjectPath;
use std::collections::BTreeMap;

/// The objects a memory provider holds. Storing or removing one moves a
/// watch, which [`super::MemoryStorage::wait_for_object`] waits on: a
/// request's trace can't say an object is there, since a request is
/// recorded as it goes out, before its object is stored.
pub(super) struct MemoryObjects {
    held: BTreeMap<ObjectPath, Object>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl MemoryObjects {
    pub(super) fn new() -> Self {
        Self { held: BTreeMap::new(), changed: tokio::sync::watch::channel(0).0 }
    }

    pub(super) fn get(&self, path: &ObjectPath) -> Option<&Object> {
        self.held.get(path)
    }

    /// The object at `path`, to change in place; its path is held either
    /// way, so the watch doesn't move.
    pub(super) fn get_mut(&mut self, path: &ObjectPath) -> Option<&mut Object> {
        self.held.get_mut(path)
    }

    pub(super) fn contains_key(&self, path: &ObjectPath) -> bool {
        self.held.contains_key(path)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &ObjectPath> {
        self.held.keys()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&ObjectPath, &Object)> {
        self.held.iter()
    }

    pub(super) fn insert(&mut self, path: ObjectPath, object: Object) {
        self.held.insert(path, object);
        self.changed.send_modify(|changes| *changes += 1);
    }

    pub(super) fn remove(&mut self, path: &ObjectPath) {
        if self.held.remove(path).is_some() {
            self.changed.send_modify(|changes| *changes += 1);
        }
    }

    /// Hears each later store or removal.
    pub(super) fn watch(&self) -> StoredWatch {
        StoredWatch(self.changed.subscribe())
    }
}

/// Hears each store or removal in a memory provider's objects after it was
/// taken.
pub(super) struct StoredWatch(tokio::sync::watch::Receiver<u64>);

impl StoredWatch {
    /// Resolves once an object is stored or removed.
    pub(super) async fn changed(&mut self) {
        self.0.changed().await.expect("the provider keeps its objects while a test holds it");
    }
}
