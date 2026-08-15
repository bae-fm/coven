use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::{CloudRuntime, CloudRuntimeError};

#[tokio::test]
async fn runtime_is_built_only_when_an_operation_runs() {
    let runtime = CloudRuntime::new();
    assert!(runtime
        .inner
        .runtime
        .lock()
        .expect("lock cloud runtime")
        .is_none());

    runtime
        .run(|| async { Ok::<_, ()>(()) })
        .await
        .expect("run cloud task")
        .expect("cloud operation succeeds");

    assert!(runtime
        .inner
        .runtime
        .lock()
        .expect("lock cloud runtime")
        .is_some());
}

#[tokio::test]
async fn nested_operations_stay_on_the_cloud_worker() {
    let runtime = CloudRuntime::new();
    let nested = runtime.clone();
    let (outer, inner) = runtime
        .run(move || async move {
            let outer = std::thread::current()
                .name()
                .expect("cloud worker has a name")
                .to_string();
            let inner = nested
                .run(|| async {
                    Ok::<_, ()>(
                        std::thread::current()
                            .name()
                            .expect("cloud worker has a name")
                            .to_string(),
                    )
                })
                .await
                .expect("run nested cloud task")?;
            Ok::<_, ()>((outer, inner))
        })
        .await
        .expect("run cloud task")
        .expect("cloud operation succeeds");

    assert_eq!(outer, "coven-cloud");
    assert_eq!(inner, outer);
}

#[tokio::test]
async fn dropping_a_run_aborts_its_owned_task() {
    struct Running(Arc<AtomicBool>);
    impl Drop for Running {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }

    let runtime = CloudRuntime::new();
    let running = Arc::new(AtomicBool::new(false));
    let operation_running = Arc::clone(&running);
    let mut operation = Box::pin(runtime.run(move || async move {
        operation_running.store(true, Ordering::SeqCst);
        let _running = Running(Arc::clone(&operation_running));
        std::future::pending::<Result<(), ()>>().await
    }));
    tokio::select! {
        _ = operation.as_mut() => panic!("pending cloud operation returned"),
        () = async {
            while !running.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    drop(operation);

    while running.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn task_panics_remain_typed_task_failures() {
    let runtime = CloudRuntime::new();
    let error = runtime
        .run(|| async move {
            panic!("cloud task panic");
            #[allow(unreachable_code)]
            Ok::<(), ()>(())
        })
        .await
        .expect_err("panicking cloud task must fail");

    match error {
        CloudRuntimeError::Task(source) => assert!(source.is_panic()),
        CloudRuntimeError::Start(source) => {
            panic!("runtime unexpectedly failed to start: {source}")
        }
    }
}

#[tokio::test]
async fn a_spawned_operation_keeps_its_runtime_alive() {
    let runtime = CloudRuntime::new();
    let (release, wait) = tokio::sync::oneshot::channel();
    let task = runtime
        .spawn(|| async move {
            wait.await.expect("release cloud task");
            std::thread::current()
                .name()
                .expect("cloud worker has a name")
                .to_string()
        })
        .expect("start cloud task");
    drop(runtime);

    release.send(()).expect("cloud task still receives release");
    assert_eq!(task.await.expect("cloud task completes"), "coven-cloud");
}
