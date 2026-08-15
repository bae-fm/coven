use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::{CloudFileReadError, CloudHomeError};

tokio::task_local! {
    static CLOUD_RUNTIME_TASK: ();
}

type CloudFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;
type CloudOperation<T, E> = Box<dyn FnOnce() -> CloudFuture<T, E> + Send + 'static>;
type CloudRun<T, E> =
    Pin<Box<dyn Future<Output = Result<Result<T, E>, CloudRuntimeError>> + Send + 'static>>;
type CloudTask<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type CloudTaskOperation<T> = Box<dyn FnOnce() -> CloudTask<T> + Send + 'static>;

/// A failure to start or execute work on Coven's cloud runtime.
#[derive(Debug, thiserror::Error)]
pub enum CloudRuntimeError {
    #[error("start Coven cloud runtime: {0}")]
    Start(#[source] std::io::Error),
    #[error("cloud operation task failed: {0}")]
    Task(#[source] tokio::task::JoinError),
}

/// The executor retained by Coven's cloud owner and the provider homes it
/// creates. The Tokio runtime is built on first use, and callers provide an
/// operation builder so its future is also constructed on the cloud worker.
#[derive(Clone)]
pub(crate) struct CloudRuntime {
    inner: Arc<CloudRuntimeInner>,
}

struct CloudRuntimeInner {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
}

impl CloudRuntime {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CloudRuntimeInner {
                runtime: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn run<T, E, F>(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> CloudRun<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: Future<Output = Result<T, E>> + Send + 'static,
    {
        self.run_erased(Box::new(move || Box::pin(operation())))
    }

    fn run_erased<T, E>(&self, operation: CloudOperation<T, E>) -> CloudRun<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let runtime = self.clone();
        Box::pin(async move {
            if CLOUD_RUNTIME_TASK.try_with(|_| ()).is_ok() {
                return Ok(operation().await);
            }

            AbortOnDropTask::new(runtime.spawn_erased(operation)?)
                .wait()
                .await
                .map_err(CloudRuntimeError::Task)
        })
    }

    pub(crate) async fn run_cloud<T, F>(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> Result<T, CloudHomeError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, CloudHomeError>> + Send + 'static,
    {
        self.run(operation)
            .await
            .map_err(|error| CloudHomeError::transport("run cloud operation", error))?
    }

    pub(crate) async fn run_file_read<T, F>(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> Result<T, CloudFileReadError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, CloudFileReadError>> + Send + 'static,
    {
        self.run(operation).await.map_err(|error| {
            CloudFileReadError::Source(CloudHomeError::transport(
                "run cloud file-read operation",
                error,
            ))
        })?
    }

    pub(crate) fn spawn<T, F>(
        &self,
        operation: impl FnOnce() -> F + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<T>, CloudRuntimeError>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        self.spawn_erased(Box::new(move || Box::pin(operation())))
    }

    fn spawn_erased<T>(
        &self,
        operation: CloudTaskOperation<T>,
    ) -> Result<tokio::task::JoinHandle<T>, CloudRuntimeError>
    where
        T: Send + 'static,
    {
        let handle = self.handle()?;
        let lifetime = self.clone();
        Ok(handle.spawn(CLOUD_RUNTIME_TASK.scope((), async move {
            let result = operation().await;
            drop(lifetime);
            result
        })))
    }

    fn handle(&self) -> Result<tokio::runtime::Handle, CloudRuntimeError> {
        let mut runtime = self.inner.runtime.lock().expect("lock cloud runtime");
        if runtime.is_none() {
            *runtime = Some(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_stack_size(16 * 1024 * 1024)
                    .thread_name("coven-cloud")
                    .enable_all()
                    .build()
                    .map_err(CloudRuntimeError::Start)?,
            );
        }
        Ok(runtime
            .as_ref()
            .expect("cloud runtime initialized above")
            .handle()
            .clone())
    }
}

impl Drop for CloudRuntimeInner {
    fn drop(&mut self) {
        if let Some(runtime) = self
            .runtime
            .get_mut()
            .expect("lock cloud runtime during final drop")
            .take()
        {
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
            .expect("cloud task handle is present")
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

#[cfg(test)]
mod tests;
