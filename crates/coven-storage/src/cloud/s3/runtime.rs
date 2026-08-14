use crate::{CloudFileReadError, CloudHomeError};

/// The runtime retained by an S3 home and every task it starts. Its worker
/// threads have the stack required by the AWS endpoint resolver.
///
/// The AWS endpoint resolver descends deeply and synchronously in one poll.
/// Running every AWS interaction here prevents that descent from overflowing a
/// host executor thread with a narrower stack.
#[derive(Clone)]
pub(crate) struct S3Runtime {
    inner: std::sync::Arc<S3RuntimeInner>,
}

struct S3RuntimeInner {
    runtime: Option<tokio::runtime::Runtime>,
}

impl S3Runtime {
    pub(crate) fn new() -> Result<Self, CloudHomeError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(16 * 1024 * 1024)
            .thread_name("coven-s3")
            .enable_all()
            .build()
            .map_err(|error| {
                CloudHomeError::transport("build coven S3 runtime".to_string(), error)
            })?;
        Ok(Self {
            inner: std::sync::Arc::new(S3RuntimeInner {
                runtime: Some(runtime),
            }),
        })
    }

    /// Construct and run an operation on the S3 runtime, keeping the runtime
    /// alive until the task completes. Multipart upload owners use this after
    /// the home that opened them drops.
    pub(crate) fn spawn<T: Send + 'static, F: std::future::Future<Output = T> + Send + 'static>(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        let runtime = self.clone();
        self.inner
            .runtime
            .as_ref()
            .expect("S3 runtime is present while its owner is alive")
            .spawn(async move {
                let result = operation().await;
                drop(runtime);
                result
            })
    }

    pub(crate) async fn run<
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, CloudHomeError>> + Send + 'static,
    >(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> Result<T, CloudHomeError> {
        self.run_with(operation, |error| {
            CloudHomeError::transport("S3 task aborted".to_string(), error)
        })
        .await
    }

    pub(crate) async fn run_file_read<
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, CloudFileReadError>> + Send + 'static,
    >(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> Result<T, CloudFileReadError> {
        self.run_with(operation, |error| {
            CloudFileReadError::Source(CloudHomeError::transport("run S3 file-read task", error))
        })
        .await
    }

    async fn run_with<
        T: Send + 'static,
        E: Send + 'static,
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    >(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
        task_error: impl FnOnce(tokio::task::JoinError) -> E,
    ) -> Result<T, E> {
        match AbortOnDropTask::new(self.spawn(operation)).wait().await {
            Ok(result) => result,
            Err(error) => Err(task_error(error)),
        }
    }
}

impl Drop for S3RuntimeInner {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // The final task may release the final owner on an S3 worker.
            // Tokio forbids its blocking Runtime drop from an async context.
            runtime.shutdown_background();
        }
    }
}

struct AbortOnDropTask<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn wait(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("S3 task handle is present")
            .await;
        self.handle.take();
        result
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
