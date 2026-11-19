// The crate `clippy::task_state_outside_task` checks under the policy of
// this folder: each line where a type other than a task, or a handle the
// task makes, holds state a task makes for its operation ends in
// `// finding`, as does each place this crate's task makes state that the
// earlier crate's holder holds.

use std::sync::{Arc, Mutex};

use tasks_base::{Lock, Plain, Shared};

pub struct Keeper {
    lock: Arc<Lock>, // finding
}

pub struct Values {
    plain: Arc<Plain>,
}

pub struct LoadHandle {
    lock: Arc<Lock>,
}

pub struct Counter {
    count: Mutex<u32>,
}

pub struct Step {
    counter: Arc<Counter>,
}

impl Step {
    pub fn start() -> Step {
        let counter = Arc::new(Counter { count: Mutex::new(0) });
        Step { counter }
    }
}

pub enum Watcher {
    Watching(Option<Arc<Counter>>), // finding
    Idle,
}

pub struct Opening {
    shared: Arc<Shared>,
}

pub fn open() -> Opening {
    Opening { shared: Arc::new(Shared::new()) } // finding
}

#[cfg(feature = "test-utils")]
pub struct Fake {
    counter: Arc<Counter>,
}
