//! The shared data rule for received row changes (§8, D5; Lean
//! `DataDependency`). A change survives only if its own admission does and every
//! change whose data it actually used survives: its generation source and each
//! cell setter it replaced, at the same row. Recorded reads, later readers and
//! loss dismissal never decide dependence.

/// The changes one row change used, in role order: the generation source
/// first, then each replaced setter in the authored column order. A source
/// that fills several roles appears once per role.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance<C> {
    /// The change that established the generation this change advanced.
    pub generation: Option<C>,
    /// The change that set each cell this change replaced.
    pub setters: Vec<C>,
}

impl<C> Provenance<C> {
    /// Every source, in role order, keeping repeats.
    pub fn sources(&self) -> impl Iterator<Item = &C> {
        self.generation.iter().chain(&self.setters)
    }
}

/// One row change's verdict under the shared rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataVerdict<C> {
    /// It applies.
    Retained,
    /// Its own admission dropped it: a retirement window, schema change or
    /// reset. That policy decides whether it leaves a loss.
    Dropped,
    /// It was admitted but used data that does not survive. It becomes a
    /// frozen loss naming these failed sources in role order, with repeats.
    Dependent(Vec<C>),
}

impl<C> DataVerdict<C> {
    /// Whether this change survives as a source for later changes.
    pub fn retained(&self) -> bool {
        matches!(self, Self::Retained)
    }
}

/// Classify a change from its own admission and its sources' verdicts.
/// Sources were decided first: each was authored, and so consumed, earlier.
/// `retained` answers for one source; an unavailable source is the caller's
/// wait or refusal, never a guess here.
pub fn classify<C: Clone, E>(
    admitted: bool,
    provenance: &Provenance<C>,
    mut retained: impl FnMut(&C) -> Result<bool, E>,
) -> Result<DataVerdict<C>, E> {
    if !admitted {
        return Ok(DataVerdict::Dropped);
    }
    let mut failed = Vec::new();
    for source in provenance.sources() {
        if !retained(source)? {
            failed.push(source.clone());
        }
    }
    Ok(if failed.is_empty() {
        DataVerdict::Retained
    } else {
        DataVerdict::Dependent(failed)
    })
}

#[cfg(test)]
#[path = "dependency_tests.rs"]
mod tests;
