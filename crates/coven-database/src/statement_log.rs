//! The statements a database's read contexts ran, for tests and
//! measurements that look at what a read asked of SQLite.

use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};

/// The statements the reads of the databases it was given to ran, in order,
/// from then on. Clones share one log, as a `WorkCounter`'s do.
#[derive(Clone)]
pub struct StatementLog(Arc<Mutex<Vec<String>>>);

impl StatementLog {
    /// A log no database has recorded into yet, for a test to give one.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    /// The recorded statements so far, each as SQLite's text with the bound
    /// values written in.
    pub fn statements(&self) -> Vec<String> {
        lock_unpoisoned(&self.0).clone()
    }

    /// The recorded statements so far, emptying the log.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut lock_unpoisoned(&self.0))
    }

    pub(crate) fn push(&self, statement: String) {
        lock_unpoisoned(&self.0).push(statement);
    }
}
