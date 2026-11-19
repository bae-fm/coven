// A crate of the workspace that the checked crate depends on: a capability
// trait and its implementation in a private module, a construction-only
// capability, and a generic type that holds what it is given.

mod time {
    pub trait Clock {}

    #[derive(Clone, Default)]
    pub struct SystemClock;

    impl Clock for SystemClock {}
}

pub mod files {
    pub struct StoreDir;

    impl StoreDir {
        pub fn open() -> StoreDir {
            StoreDir
        }
    }
}

pub use time::{Clock, SystemClock};

pub struct Wrapper<T>(T);

impl<T> Wrapper<T> {
    pub fn new(value: T) -> Self {
        Wrapper(value)
    }
}
