//! Per-audience starting slots during one atomic reload.
use crate::{snapshot_error::invalid, DbError};
use coven_merge::{Audience, Slot};
use std::collections::BTreeMap;
pub(crate) struct SnapshotCoverage {
    audiences: BTreeMap<Audience, u64>,
    pub(crate) target: u64,
    /// A rebuild's: the last slot whose store parts it reads, and that a
    /// write above its database's version is refused, not waited for (§18.3).
    store_through: Option<u64>,
}
impl SnapshotCoverage {
    pub(crate) fn new(target: u64) -> Self {
        Self { audiences: BTreeMap::new(), target, store_through: None }
    }
    /// The coverage of a rebuild, which reads the store only through slot
    /// `store_through`: its rows after the store's raise past the circle's
    /// version are rows the circle's version can't hold (§18.3).
    pub(crate) fn rebuilding(target: u64, store_through: u64) -> Self {
        Self { store_through: Some(store_through), ..Self::new(target) }
    }
    pub(crate) fn is_rebuild(&self) -> bool {
        self.store_through.is_some()
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
        if *audience == Audience::Store && self.store_through.is_some_and(|last| slot.get() > last)
        {
            return false;
        }
        self.audiences.get(audience).is_some_and(|covers| *covers < slot.get())
    }
}
