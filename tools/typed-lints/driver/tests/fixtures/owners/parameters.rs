// The crate `clippy::method_takes_capability` checks under the policy of this
// folder: each line where a function of an impl, no constructor or
// composition root, takes a construction-only capability or a handle ends in
// `// finding`.

use std::sync::Arc;

use owners_base::SystemClock;
use owners_base::files::StoreDir;

pub struct Query;
pub struct Requests;
pub struct View {
    requests: Requests,
}
pub struct Rows {
    store_dir: StoreDir,
}
pub struct Record {
    id: u64,
}
/// Holds what it is given: with a store directory, an owner.
pub struct Holder<T> {
    value: T,
}

impl View {
    pub fn new(requests: Requests) -> Self {
        View { requests }
    }

    pub async fn open(requests: Requests) -> Result<Self, ()> {
        Ok(View { requests })
    }

    pub fn assemble(requests: Requests) {
        drop(requests);
    }

    pub fn replace(&mut self, /* finding */ requests: Requests) {
        self.requests = requests;
    }
}

impl Query {
    pub fn accept(&self, requests: Requests) {} // finding
}

impl Rows {
    pub fn new(store_dir: StoreDir) -> Arc<Self> {
        Arc::new(Rows { store_dir })
    }

    pub fn execute(&self, store_dir: &StoreDir) {} // finding

    pub fn clock(&self, clock: &SystemClock) {} // finding

    pub fn consume(&self, run: impl FnOnce(&StoreDir)) {}

    pub fn consume_boxed(&self, run: Box<dyn Fn(&StoreDir)>) {}

    pub fn consume_pointer(&self, run: fn(&StoreDir)) {}

    pub fn supply(&self, build: impl FnOnce() -> StoreDir) {} // finding

    pub fn supply_bound<F: FnOnce() -> StoreDir>(&self, build: F) {} // finding

    pub fn supply_pointer(&self, build: fn() -> StoreDir) {} // finding

    pub fn mixed(&self, run: (fn(&StoreDir), StoreDir)) {} // finding

    pub fn many(&self, store_dirs: Vec<StoreDir>) {} // finding

    pub fn maybe(&self, requests: Option<Requests>) {} // finding

    pub fn holder(&self, holder: &Holder<StoreDir>) {}

    #[expect(clippy::method_takes_capability, reason = "the fixture's mark")]
    pub fn marked(&self, store_dir: StoreDir) {}
}

impl Record {
    pub fn read(store_dir: &StoreDir, id: u64) -> Self {
        Record { id }
    }

    pub fn count(/* finding */ store_dir: &StoreDir) -> u64 {
        0
    }

    #[cfg(feature = "test-utils")]
    pub fn fixture(&self, store_dir: StoreDir) {}
}

pub fn insert(store_dir: &StoreDir, record: &Record) {}
