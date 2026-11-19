// The crate `clippy::built_outside_composition_root` checks under the policy
// of this folder: each line that builds an owner, a construction-only
// capability or a handle outside a composition root ends in `// finding`.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use owners_base::files::StoreDir;
use owners_base::{SystemClock, Wrapper};

pub struct Builder;
pub struct Service {
    clock: SystemClock,
}
pub struct Counter {
    count: AtomicU64,
}
pub struct Plain {
    value: u32,
}
/// Holds a lock by value, and `Shared` holds it through an `Arc`: the lock
/// is shared, so `Locked` is an owner.
pub struct Locked {
    state: Mutex<u32>,
}
pub struct Shared {
    inner: Arc<Locked>,
}
pub struct Task {
    counter: Counter,
}
pub struct Query;
pub struct Requests;
pub struct Directory;
/// Holds its clocks in a `Vec`, which keeps them behind a raw pointer.
pub struct Clocks {
    clocks: Vec<SystemClock>,
}

impl Builder {
    pub fn open() -> Service {
        let _ = Counter { count: AtomicU64::new(0) };
        let _ = || SystemClock;
        Service { clock: SystemClock }
    }

    pub fn run() {
        let clock = SystemClock; // finding
        let _ = Wrapper::new(clock);
        let _ = Counter { count: AtomicU64::new(0) }; // finding
        let _ = Plain { value: 1 };
        let _ = Query.requests();
        let _ = Requests; // finding
        let _ = StoreDir::open(); // finding
        let _ = Arc::new(Locked { state: Mutex::new(0) }); // finding
        let _ = <SystemClock as Default>::default(); // finding
        let _ = Task { counter: Counter { count: AtomicU64::new(0) } }; // finding
        let _ = || SystemClock; // finding
        let _ = Clocks { clocks: Vec::new() }; // finding
    }

    pub fn borrow(service: &Service, shared: &Shared, task: &Task) -> u32 {
        let _ = (&service.clock, &shared.inner, &task.counter.count);
        Plain { value: 0 }.value
    }
}

impl Query {
    pub fn requests(&self) -> Requests {
        Requests
    }
}

impl Requests {
    pub fn again(&self) -> Requests {
        Requests
    }
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self { clock: self.clock.clone() }
    }
}

impl Directory {
    pub fn file(&self) -> StoreDir {
        StoreDir::open()
    }

    pub fn other(&self) -> StoreDir {
        StoreDir::open() // finding
    }
}

fn make_dir() -> StoreDir {
    StoreDir::open()
}

pub fn uses() {
    let _ = make_dir(); // finding
    let _ = Directory.file();
}

#[expect(clippy::built_outside_composition_root, reason = "the fixture's mark")]
pub fn marked() -> Counter {
    Counter { count: AtomicU64::new(0) }
}

pub static REGISTRY: Mutex<u32> = Mutex::new(0);
pub static NEXT: AtomicU64 = AtomicU64::new(0); // finding
pub static PLAIN: u32 = 1;
pub static INNER: Plain = Plain { value: 2 };
pub static HELD: Holder = // finding
    Holder { count: AtomicU64::new(0) }; // finding
pub struct Holder {
    count: AtomicU64,
}

/// An owner a builder's steps hand on whole.
pub struct Building {
    counter: Counter,
}

impl Building {
    pub fn step(self) -> Building {
        self
    }
}

pub fn steps(building: Building) -> Building {
    building.step()
}

pub fn with_test_only_code() {
    #[cfg(feature = "test-utils")]
    let _ = SystemClock;
}

/// What another platform builds: the policy's row names a type in it.
#[cfg(feature = "another-platform")]
mod elsewhere {
    pub struct Task;
}

#[cfg(feature = "test-utils")]
pub fn fixture() -> Service {
    Service { clock: SystemClock }
}
