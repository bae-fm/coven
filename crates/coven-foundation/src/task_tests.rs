use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::*;

/// Sets its flag when dropped: the cleanup a caller's work holds.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn blocking_returns_the_work_s_value() {
    assert_eq!(blocking("add", || 2 + 2).await, Ok(4));
}

#[tokio::test]
async fn blocking_turns_a_panic_into_panicked_with_its_message() {
    let failed = blocking("read the journal", || -> u8 { panic!("journal is empty") })
        .await
        .expect_err("a panicking closure gives no value");
    assert_eq!(
        failed,
        TaskFailed::Panicked {
            operation: "read the journal",
            message: "journal is empty".to_string(),
        }
    );
    assert_eq!(failed.to_string(), "read the journal panicked: journal is empty");
}

#[tokio::test]
async fn joined_turns_a_cancelled_task_into_shutdown() {
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let failed = joined("wait forever", task).await.expect_err("an aborted task gives no value");
    assert_eq!(failed, TaskFailed::Shutdown { operation: "wait forever" });
    assert!(!failed.is_panic());
}

#[tokio::test]
async fn joined_carries_a_formatted_panic_message() {
    let task = tokio::spawn(async {
        let index = 7;
        panic!("no window {index}");
    });
    let failed = joined("fill the read-ahead", task).await.expect_err("panicked");
    assert_eq!(
        failed,
        TaskFailed::Panicked {
            operation: "fill the read-ahead",
            message: "no window 7".to_string(),
        }
    );
}

#[tokio::test]
async fn caught_fails_the_work_and_its_held_cleanup_runs() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let guard = SetOnDrop(cleaned.clone());
    let failed = caught("apply one slot", async move {
        let _guard = guard;
        tokio::task::yield_now().await;
        panic!("row count went negative");
    })
    .await
    .expect_err("the work panicked");
    assert_eq!(failed.operation(), "apply one slot");
    assert!(failed.is_panic());
    assert!(cleaned.load(Ordering::SeqCst), "the work's drop guard ran");
}

#[tokio::test]
async fn caught_returns_the_work_s_value_across_polls() {
    let value = caught("count", async {
        tokio::task::yield_now().await;
        3
    })
    .await;
    assert_eq!(value, Ok(3));
}

/// A future that panics when polled and again when dropped, as a hand-written
/// future does whose fields an earlier panic left inconsistent: unwinding out
/// of `poll` leaves its fields in place, so they drop later, outside the
/// unwind.
struct PanicsTwice;

impl Future for PanicsTwice {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        panic!("decoder state is wrong");
    }
}

impl Drop for PanicsTwice {
    fn drop(&mut self) {
        panic!("the lock this future releases is poisoned");
    }
}

#[tokio::test]
async fn caught_keeps_a_panicking_drop_of_the_failed_work_from_unwinding() {
    let failed = caught("decode", PanicsTwice).await;
    assert_eq!(
        failed,
        Err(TaskFailed::Panicked {
            operation: "decode",
            message: "decoder state is wrong".to_string(),
        })
    );
}

#[test]
fn caught_sync_turns_a_panic_into_panicked() {
    let failed =
        caught_sync("read callback", || -> i32 { panic!("buffer gone") }).expect_err("panicked");
    assert_eq!(
        failed,
        TaskFailed::Panicked { operation: "read callback", message: "buffer gone".to_string() }
    );
    assert_eq!(caught_sync("read callback", || 5), Ok(5));
}

#[test]
fn panic_message_names_a_payload_that_is_not_text() {
    let payload: Box<dyn Any + Send> = Box::new(42_u32);
    assert_eq!(panic_message(payload.as_ref()), "a panic whose payload is not text");
}

/// A panic payload whose own drop panics, as `std::panic::panic_any` can
/// throw.
struct PanicsWhenDropped;

impl Drop for PanicsWhenDropped {
    fn drop(&mut self) {
        panic!("the payload's drop panicked");
    }
}

#[test]
fn caught_sync_keeps_a_payload_whose_drop_panics_from_unwinding() {
    let failed =
        caught_sync("seek callback", || -> i32 { std::panic::panic_any(PanicsWhenDropped) });
    assert_eq!(
        failed,
        Err(TaskFailed::Panicked {
            operation: "seek callback",
            message: "a panic whose payload is not text".to_string(),
        })
    );
}

#[tokio::test]
async fn joined_keeps_a_payload_whose_drop_panics_from_unwinding() {
    let task = tokio::spawn(async { std::panic::panic_any(PanicsWhenDropped) });
    let failed = joined("upload a file", task).await;
    assert_eq!(
        failed,
        Err(TaskFailed::Panicked {
            operation: "upload a file",
            message: "a panic whose payload is not text".to_string(),
        })
    );
}
