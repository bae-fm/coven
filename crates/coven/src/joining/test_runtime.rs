/// Run a joining test body on a thread with room for the flow's poll frames.
///
/// The joining futures are not `Send`, so the current-thread runtime moves to
/// the configured thread rather than moving the future to a Tokio worker.
pub(super) fn on_a_deep_stack<Body, Fut>(body: Body)
where
    Body: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build the test runtime")
                .block_on(body());
        })
        .expect("spawn the test thread")
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}
