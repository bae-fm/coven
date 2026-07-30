use crate::storage::{CloudFileReadError, CloudHomeError};

/// The runtime retained by an S3 home and every task it starts. Its worker
/// threads have the stack required by the AWS endpoint resolver.
///
/// The AWS endpoint resolver descends deeply and synchronously in one poll.
/// Running every AWS interaction here prevents that descent from overflowing a
/// host executor thread with a narrower stack.
#[derive(Clone)]
pub(super) struct S3Runtime {
    inner: std::sync::Arc<S3RuntimeInner>,
}

struct S3RuntimeInner {
    runtime: Option<tokio::runtime::Runtime>,
}

impl S3Runtime {
    pub(super) fn new() -> Result<Self, CloudHomeError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(16 * 1024 * 1024)
            .thread_name("coven-s3")
            .enable_all()
            .build()
            .map_err(|error| {
                CloudHomeError::Transport(format!("build coven S3 runtime: {error}"))
            })?;
        Ok(Self {
            inner: std::sync::Arc::new(S3RuntimeInner {
                runtime: Some(runtime),
            }),
        })
    }

    /// Spawn a task that keeps this runtime alive until the task completes.
    /// Multipart upload owners use this after the home that opened them drops.
    pub(super) fn spawn<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        let runtime = self.clone();
        self.inner
            .runtime
            .as_ref()
            .expect("S3 runtime is present while its owner is alive")
            .spawn(async move {
                let result = future.await;
                drop(runtime);
                result
            })
    }

    pub(super) async fn run<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, CloudHomeError>> + Send + 'static,
    ) -> Result<T, CloudHomeError> {
        self.run_with(future, |error| {
            CloudHomeError::Transport(format!("S3 task aborted: {error}"))
        })
        .await
    }

    pub(super) async fn run_file_read<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, CloudFileReadError>> + Send + 'static,
    ) -> Result<T, CloudFileReadError> {
        self.run_with(future, |error| {
            CloudFileReadError::Source(CloudHomeError::Transport(format!(
                "S3 task aborted: {error}"
            )))
        })
        .await
    }

    async fn run_with<T: Send + 'static, E: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, E>> + Send + 'static,
        task_error: impl FnOnce(tokio::task::JoinError) -> E,
    ) -> Result<T, E> {
        match AbortOnDropTask::new(self.spawn(future)).wait().await {
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
