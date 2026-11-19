//! Stands in for uniffi in the lints' tests: the lints know the function that
//! runs an exported async function's future by the path
//! `uniffi::rust_future_new`, which uniffi re-exports from a module of its
//! core through globs, as here.

mod ffi {
    mod rustfuture {
        use std::future::Future;

        /// The future's handle, which the foreign caller polls.
        pub struct Handle;

        pub fn rust_future_new<F, T, UT>(future: F, _tag: UT) -> Handle
        where
            F: Future<Output = T> + Send + 'static,
        {
            drop(future);
            Handle
        }
    }

    pub use rustfuture::*;
}

pub use ffi::*;
