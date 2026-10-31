//! The steps of opening a store for writing, which a host draws a bar for
//! (§4, api.md E1).

/// A step of opening a store for writing (§4), in the order they run. A
/// step runs only when it has work to do, so most opens report none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenStep {
    /// Rebuilding the indexes a new version of SQLite, or of one of the
    /// app's collations or functions, changed. Done and total count
    /// rebuilds: one for each index, and one for all the indexes in a
    /// collation, which SQLite rebuilds together.
    Rebuilding,
    /// Running the app's migrations the store hasn't run yet. Done and total
    /// count migrations.
    Migrating,
}

#[cfg(test)]
#[path = "open_step_tests.rs"]
mod tests;
