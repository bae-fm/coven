//! Bounded-stack execution for CPU work reached through nested async protocol verification.

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockingTaskError {
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("blocking task returned an invalid result type")]
    ResultType,
}

pub(crate) async fn run<T>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, BlockingTaskError>
where
    T: Send + 'static,
{
    let work: Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send> =
        Box::new(move || Box::new(work()));
    let result = tokio::task::spawn_blocking(work).await?;
    result
        .downcast::<T>()
        .map(|result| *result)
        .map_err(|_| BlockingTaskError::ResultType)
}
