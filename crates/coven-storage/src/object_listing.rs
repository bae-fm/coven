use crate::{CloudProvider, ObjectPath, ObjectPrefix, StorageError, StorageFailure, StoredObject};
use std::collections::BTreeMap;

/// The connection's listing result. Native providers add entries while consuming
/// pages so a conflict stops the request before fetching another page.
///
/// A store's folder or prefix can also hold things coven never wrote: a
/// person's stray files and folders, native documents, another store's
/// folder, or a name close to a coven path. D10 gives each object exactly
/// one spelling and no reader looks anywhere else, so such an entry is no
/// object of this store and the listing skips it. What the provider itself
/// gets wrong — an entry outside the requested scope, missing metadata, a
/// failed or incomplete page — still fails the listing.
pub struct ObjectListing {
    prefix: ObjectPrefix,
    provider: CloudProvider,
    objects: BTreeMap<ObjectPath, StoredObject>,
    skipped: usize,
}

impl ObjectListing {
    pub(crate) fn new(prefix: ObjectPrefix, provider: CloudProvider) -> Self {
        Self {
            prefix,
            provider,
            objects: BTreeMap::new(),
            skipped: 0,
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

    /// The object path of a native file listed at `name`, relative to the
    /// location and starting with the store id. `None` is an entry outside
    /// D10, which the listing skips; a name outside the requested prefix is
    /// the provider's error.
    pub(crate) fn object_path(&mut self, name: &str) -> Result<Option<ObjectPath>, StorageError> {
        self.within(name)?;
        let path = ObjectPath::parse(name).ok();
        self.skipped += usize::from(path.is_none());
        Ok(path)
    }

    /// Whether a native folder listed at `name` can hold this store's
    /// objects, so a recursive listing descends into it. A folder outside
    /// D10's directories holds none and is skipped with its contents.
    pub(crate) fn object_folder(&mut self, name: &str) -> Result<bool, StorageError> {
        if name == self.prefix.as_str().trim_end_matches('/') {
            // A recursive listing can include the requested folder itself.
            return Ok(true);
        }
        self.within(name)?;
        let folder = crate::path::validate_directory(name).is_ok();
        self.skipped += usize::from(!folder);
        Ok(folder)
    }

    /// Count a native entry that cannot be an object at all, such as a
    /// document, shortcut or package, whatever its name.
    pub(crate) fn skip(&mut self) {
        self.skipped += 1;
    }

    fn within(&self, name: &str) -> Result<(), StorageError> {
        if name.starts_with(self.prefix.as_str()) {
            Ok(())
        } else {
            Err(StorageFailure::Protocol.with_source("provider listed outside prefix"))
        }
    }

    pub(crate) fn finish(self) -> Vec<StoredObject> {
        if self.skipped > 0 {
            tracing::debug!(
                prefix = self.prefix.as_str(),
                skipped = self.skipped,
                "listing skipped entries outside the store layout"
            );
        }
        self.objects.into_values().collect()
    }
}

#[cfg(test)]
#[path = "object_listing_tests.rs"]
mod tests;
