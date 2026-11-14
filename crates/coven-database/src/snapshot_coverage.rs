//! Per-audience starting slots during one atomic reload.
use crate::{snapshot_error::invalid, DbError};
use coven_merge::{Audience, Slot};
use std::collections::BTreeMap;
/// What a rebuild's load is told about the app besides its slots.
pub(crate) struct Rebuilding {
    /// The last slot whose store parts the rebuild reads.
    pub(crate) store_through: u64,
    /// The versions of the app's breaking migrations, which the rebuild's
    /// database, at an older version, doesn't know.
    pub(crate) breaking: Vec<u32>,
}
pub(crate) struct SnapshotCoverage {
    audiences: BTreeMap<Audience, u64>,
    pub(crate) target: u64,
    /// A rebuild's: what it reads of the store, and the breaking versions a
    /// write above its database's version is kept as a lost value for, not
    /// waited for (§18.3).
    rebuilding: Option<Rebuilding>,
    /// The earliest slot an audience left for its rebuild is counted from: the
    /// slots start after it, though none of that audience's parts are read.
    gated_from: Option<u64>,
}
impl SnapshotCoverage {
    pub(crate) fn new(target: u64) -> Self {
        Self { audiences: BTreeMap::new(), target, rebuilding: None, gated_from: None }
    }
    /// An audience left for its rebuild, whose rows count through `covers`.
    pub(crate) fn gated(&mut self, covers: u64) -> Result<(), DbError> {
        if covers > self.target {
            return Err(invalid("snapshot coverage exceeds reload target"));
        }
        self.gated_from = Some(self.gated_from.map_or(covers, |from| from.min(covers)));
        Ok(())
    }
    /// The coverage of a rebuild, which reads the store only through slot
    /// `rebuilding.store_through`: its rows after the store's raise past the
    /// circle's version are rows the circle's version can't hold (§18.3).
    pub(crate) fn rebuilding(target: u64, rebuilding: Rebuilding) -> Self {
        Self { rebuilding: Some(rebuilding), ..Self::new(target) }
    }
    /// The breaking version a write at schema `version` above the rebuild's
    /// database needs, which its changes are kept as lost values for: the
    /// newest breaking migration of the app at or below `version`. None for a
    /// load that isn't a rebuild.
    pub(crate) fn needed(&self, version: u32) -> Option<u32> {
        let rebuilding = self.rebuilding.as_ref()?;
        rebuilding.breaking.iter().copied().filter(|breaking| *breaking <= version).max()
    }
    pub(crate) fn is_rebuild(&self) -> bool {
        self.rebuilding.is_some()
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
        self.audiences
            .values()
            .copied()
            .chain(self.gated_from)
            .min()
            .ok_or_else(|| invalid("reload has no audiences"))
    }
    pub(crate) fn needs(&self, audience: &Audience, slot: Slot) -> bool {
        if *audience == Audience::Store
            && self.rebuilding.as_ref().is_some_and(|r| slot.get() > r.store_through)
        {
            return false;
        }
        self.audiences.get(audience).is_some_and(|covers| *covers < slot.get())
    }
}
