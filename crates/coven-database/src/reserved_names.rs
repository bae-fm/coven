//! The file names one open store reserves while their bytes wait for a
//! write (Appendix C.3).

use crate::observation::CommitObserver;
use crate::DbError;
use coven_foundation::files::FileName;
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::Bug;
use std::collections::BTreeSet;
use std::sync::Mutex;

/// The names of copies reserved in one open store: staged app bytes, cache
/// files and prepared pins. Removals skip them until the write that takes
/// them decides or their holder drops.
pub(crate) struct ReservedNames {
    names: Mutex<BTreeSet<FileName>>,
    /// The handle a release that finds the set wrong fails.
    handle: CommitObserver,
}

impl ReservedNames {
    pub(crate) fn new(handle: CommitObserver) -> Self {
        Self { names: Mutex::new(BTreeSet::new()), handle }
    }

    /// Reserve `names`.
    pub(crate) fn reserve(&self, names: impl IntoIterator<Item = FileName>) {
        lock_unpoisoned(&self.names).extend(names);
    }

    /// Let `names` go. Every name goes before any is checked, so the set is
    /// whole whatever the check finds, and the lock is taken as it is. A name
    /// that isn't reserved was released by another holder, so the set no
    /// longer says which bytes removals must skip: the store's handle fails
    /// with the bug, which goes to the call releasing them or the bug sink
    /// (E15). Holders release as they drop, perhaps during an unwind, so
    /// this never panics.
    pub(crate) fn release<'a>(&self, names: impl IntoIterator<Item = &'a FileName>) {
        let unreserved = {
            let mut reserved = lock_unpoisoned(&self.names);
            names.into_iter().filter(|name| !reserved.remove(*name)).count()
        };
        if unreserved > 0 {
            self.handle.fail_with_bug(Bug {
                operation: "release reserved file names",
                message: format!("{unreserved} released names were not reserved"),
            });
        }
    }

    /// Refuse prepared work unless this store still reserves each name it
    /// brings: work prepared by another open store, or by this one before
    /// it closed, isn't held here (`PreparedElsewhere`).
    pub(crate) fn held_here<'a>(
        &self,
        names: impl IntoIterator<Item = &'a FileName>,
    ) -> Result<(), DbError> {
        let reserved = lock_unpoisoned(&self.names);
        if names.into_iter().any(|name| !reserved.contains(name)) {
            return Err(DbError::PreparedElsewhere);
        }
        Ok(())
    }

    /// Run `read` over the reserved names, held for its run.
    pub(crate) fn with_names<T>(&self, read: impl FnOnce(&BTreeSet<FileName>) -> T) -> T {
        read(&lock_unpoisoned(&self.names))
    }
}

#[cfg(test)]
#[path = "reserved_names_tests.rs"]
mod tests;
