//! Bounded-stack execution for CPU work reached through nested async protocol verification.

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockingTaskError {
    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub(crate) async fn run<T>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, BlockingTaskError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(BlockingTaskError::Join)
}
