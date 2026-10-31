//! Apple's file protection classes have no counterpart off Apple platforms
//! (api.md E1): there a layout gives its stores' files no class, and each
//! step does nothing.

use std::path::Path;

use super::FileError;

/// No class to give a store's files.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Protection;

impl Protection {
    /// A layout that gives its stores' files no class.
    pub(crate) fn none() -> Self {
        Self
    }

    /// Nothing to give a new store directory.
    pub(crate) fn begin(self, _directory: &Path) -> Result<(), FileError> {
        Ok(())
    }

    /// Nothing to give an existing store directory.
    pub(crate) fn apply(self, _store: &Path) -> Result<(), FileError> {
        Ok(())
    }
}
