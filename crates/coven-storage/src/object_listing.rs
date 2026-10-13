use crate::{CloudProvider, ObjectPath, ObjectPrefix, StorageError, StorageFailure, StoredObject};
use std::collections::BTreeMap;

/// The connection's listing result. Native providers add entries while consuming
/// pages so a conflict stops the request before fetching another page.
pub struct ObjectListing {
    prefix: ObjectPrefix,
    provider: CloudProvider,
    objects: BTreeMap<ObjectPath, StoredObject>,
}

impl ObjectListing {
    pub(crate) fn new(prefix: ObjectPrefix, provider: CloudProvider) -> Self {
        Self {
            prefix,
            provider,
            objects: BTreeMap::new(),
        }
    }

    /// Requested object namespace, beginning with the store id within the provider location.
    pub fn prefix(&self) -> &ObjectPrefix {
        &self.prefix
    }

    /// Merge a native entry. HTTP pages may repeat identical entries; CloudKit's
    /// native queries promise unique paths. Conflicting metadata always fails.
    pub fn insert(&mut self, object: StoredObject) -> Result<(), StorageError> {
        object.check(&object.path)?;
        if !self.prefix.contains(&object.path) {
            return Err(StorageFailure::Protocol.with_source("provider listed outside prefix"));
        }
        if let Some(previous) = self.objects.get(&object.path) {
            if self.provider == CloudProvider::CloudKit || previous != &object {
                return Err(
                    StorageFailure::Protocol.with_source("provider listed conflicting objects")
                );
            }
        }
        self.objects.insert(object.path.clone(), object);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<StoredObject> {
        self.objects.into_values().collect()
    }
}
