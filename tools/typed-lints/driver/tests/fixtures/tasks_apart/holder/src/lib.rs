// A holder of the state that the task crate's task makes, which the compile
// of joined, the first crate that sees both, reports: the line that holds it
// ends in `// finding`.

use std::sync::Arc;

use apart_state::Lock;

pub struct Keeper {
    lock: Arc<Lock>, // finding
}
