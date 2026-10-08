use crate::{StorageError, StorageFailure};
use std::collections::BTreeSet;

/// One native listing's continuation history. Providers interpret their own
/// end-of-list fields; every continuation must advance, including across cycles.
pub(super) struct Pagination {
    seen: BTreeSet<String>,
}

impl Pagination {
    pub(super) fn new() -> Self {
        Self {
            seen: BTreeSet::new(),
        }
    }

    pub(super) fn check(&mut self, token: &str) -> Result<(), StorageError> {
        if !self.seen.insert(token.to_owned()) {
            return Err(StorageFailure::Protocol.with_source("repeated provider page token"));
        }
        Ok(())
    }
}
