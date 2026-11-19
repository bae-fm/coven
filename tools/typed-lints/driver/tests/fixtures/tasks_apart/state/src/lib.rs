// The state that a task of one crate makes and a holder of another holds.

use std::sync::Mutex;

pub struct Lock {
    state: Mutex<u32>,
}

impl Lock {
    pub fn new() -> Lock {
        Lock { state: Mutex::new(0) }
    }
}
