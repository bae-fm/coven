// A task that makes its state for its operation.

use std::sync::Arc;

use apart_state::Lock;

pub struct Load {
    lock: Arc<Lock>,
}

pub fn load() -> Load {
    Load { lock: Arc::new(Lock::new()) }
}
