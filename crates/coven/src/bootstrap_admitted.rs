//! The store a join publishes once it finds its admission, which registered
//! its device: its first load, from nothing, before the call returns it
//! (§13.2).

use super::*;

/// Load the store a join published once it found its admission, reporting
/// loading's steps as `Loading` (§13.2, E10). A failure, its storage access
/// lost among them, returns the open handle with it: the store is published,
/// and its sync goes on from there as any store's does.
pub(super) async fn load_joined(
    handle: CovenHandle,
    status: &impl Fn(BootstrapStatus),
    clock: &ClockRef,
    progress: watch::Sender<Option<Progress<coven_database::LoadStep>>>,
    watch: watch::Receiver<Option<Progress<coven_database::LoadStep>>>,
) -> Result<CovenHandle, BootstrapError> {
    let loading = clock.elapsed();
    let loaded = coven_foundation::progress::forward(watch, handle.load_joined(progress), |step| {
        status(BootstrapStatus::Loading(step))
    })
    .await;
    tracing::info!(
        loaded = loaded.is_ok(),
        time = ?(clock.elapsed() - loading),
        "loaded the store a join published"
    );
    match loaded {
        Ok(()) => Ok(handle),
        Err(source) => Err(BootstrapError::Published { handle, source: Box::new(source.into()) }),
    }
}
