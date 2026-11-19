// A crate of the workspace that the checked crate depends on: a task that
// makes its state for its operation in a function no other crate can call,
// one that makes a plain value, and a holder of state the checked crate's
// task makes.

use std::sync::{Arc, Mutex};

pub struct Lock {
    state: Mutex<u32>,
}

pub struct Load {
    lock: Arc<Lock>,
}

fn start() -> Load {
    Load { lock: Arc::new(Lock { state: Mutex::new(0) }) }
}

pub fn load() -> Load {
    start()
}

pub struct Plain {
    value: u32,
}

pub struct Copying {
    plain: Arc<Plain>,
}

impl Copying {
    pub fn start() -> Copying {
        Copying { plain: Arc::new(Plain { value: 0 }) }
    }
}

pub struct Shared {
    state: Mutex<u8>,
}

impl Shared {
    pub fn new() -> Shared {
        Shared { state: Mutex::new(0) }
    }
}

pub struct Registry {
    shared: Arc<Shared>,
}
