//! Per-audience starting slots during one atomic reload.
use crate::{snapshot_error::invalid, DbError};
use coven_merge::{Audience, Slot};
use std::collections::BTreeMap;
pub(crate) struct SnapshotCoverage {
    audiences: BTreeMap<Audience, u64>,
    pub(crate) target: u64,
}
impl SnapshotCoverage {
    pub(crate) fn new(target: u64) -> Self {
        Self { audiences: BTreeMap::new(), target }
    }
    pub(crate) fn loaded(&mut self, audience: Audience, covers: u64) -> Result<(), DbError> {
        if covers > self.target {
            return Err(invalid("snapshot coverage exceeds reload target"));
        }
        if self.audiences.insert(audience, covers).is_some() {
            return Err(invalid("reload repeats an audience"));
        }
        Ok(())
    }
    pub(crate) fn first(&self) -> Result<u64, DbError> {
        self.audiences.values().copied().min().ok_or_else(|| invalid("reload has no audiences"))
    }
    pub(crate) fn needs(&self, audience: &Audience, slot: Slot) -> bool {
        self.audiences.get(audience).is_some_and(|covers| *covers < slot.0)
    }
}
