//! Where `clippy::async_export_off_runtime` finds an async export that runs
//! its work on its caller's executor. A signature that ends in a `finding`
//! comment holds one finding, and no other line holds any. The policy in
//! this fixture's workspace root (`async_exports/`) lists the runs.
//!
//! `scaffolding` stands in for what uniffi's `export` macro writes: a
//! function per export that hands the export's future to
//! `uniffi::rust_future_new`, or calls a synchronous export at once.

use std::future::Future;
use std::sync::Arc;

pub mod runtime {
    use std::future::Future;

    /// The runtime the handle owns.
    #[derive(Clone)]
    pub struct OperationRuntime;

    impl OperationRuntime {
        pub async fn run<F: Future>(self, work: F) -> F::Output {
            work.await
        }

        /// Not one of the policy's runs, though it runs the work too.
        pub async fn run_here<F: Future>(self, work: F) -> F::Output {
            work.await
        }
    }
}

use runtime::OperationRuntime;

pub struct Handle {
    runtime: OperationRuntime,
    operations: OperationRuntime,
}

/// A type of another kind, with a method named as a run is.
pub struct Queue;

impl Queue {
    pub async fn run(&self) {}
}

impl Handle {
    pub async fn on_the_runtime(&self) -> u8 {
        self.runtime.clone().run(async { 1 }).await
    }

    pub async fn on_a_runtime_named_otherwise(&self) -> u8 {
        let owned = self.operations.clone();
        owned.run(async { 2 }).await
    }

    pub async fn through_run_exported(self: Arc<Self>) -> u8 {
        self.run_exported(async { 3 }).await
    }

    pub async fn on_the_callers_executor(&self) -> u8 /* finding */ {
        // `self.runtime.run(...)` would run it on the runtime.
        let _ = "run_exported";
        4
    }

    pub async fn through_another_run(&self) -> u8 /* finding */ {
        Queue.run().await;
        self.runtime.clone().run_here(async { 5 }).await
    }

    #[expect(clippy::async_export_off_runtime, reason = "the test of a mark")]
    pub async fn marked(&self) -> u8 {
        6
    }

    pub fn synchronous(&self) -> u8 {
        7
    }

    /// An async method no scaffolding calls: uniffi doesn't export it.
    pub async fn not_exported(&self) -> u8 {
        8
    }

    async fn run_exported<F: Future<Output = u8>>(self: Arc<Self>, work: F) -> u8 {
        self.runtime.clone().run(work).await
    }
}

pub async fn free_on_the_callers_executor() -> u8 /* finding */ {
    9
}

pub async fn free_on_the_runtime(runtime: OperationRuntime) -> u8 {
    runtime.run(async { 10 }).await
}

/// What uniffi's `export` macro writes for each export.
pub mod scaffolding {
    use super::*;

    pub fn on_the_runtime(handle: Arc<Handle>) -> uniffi::Handle {
        uniffi::rust_future_new(async move { handle.on_the_runtime().await }, ())
    }

    pub fn on_a_runtime_named_otherwise(handle: Arc<Handle>) -> uniffi::Handle {
        uniffi::rust_future_new(async move { handle.on_a_runtime_named_otherwise().await }, ())
    }

    pub fn through_run_exported(handle: Arc<Handle>) -> uniffi::Handle {
        uniffi::rust_future_new(async move { handle.through_run_exported().await }, ())
    }

    pub fn on_the_callers_executor(handle: Arc<Handle>) -> uniffi::Handle {
        let lifted: Result<(Arc<Handle>,), ()> = Ok((handle,));
        uniffi::rust_future_new(
            async move {
                match lifted {
                    Ok(arguments) => Ok(arguments.0.on_the_callers_executor().await),
                    Err(error) => Err(error),
                }
            },
            (),
        )
    }

    pub fn through_another_run(handle: Arc<Handle>) -> uniffi::Handle {
        uniffi::rust_future_new(async move { handle.through_another_run().await }, ())
    }

    pub fn marked(handle: Arc<Handle>) -> uniffi::Handle {
        uniffi::rust_future_new(async move { handle.marked().await }, ())
    }

    pub fn synchronous(handle: Arc<Handle>) -> u8 {
        handle.synchronous()
    }

    pub fn free_on_the_callers_executor() -> uniffi::Handle {
        uniffi::rust_future_new(async move { super::free_on_the_callers_executor().await }, ())
    }

    pub fn free_on_the_runtime() -> uniffi::Handle {
        uniffi::rust_future_new(
            async move { super::free_on_the_runtime(OperationRuntime).await },
            (),
        )
    }
}
