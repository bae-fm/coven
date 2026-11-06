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
#[expect(clippy::disallowed_methods, reason = "a test of `blocking` itself")]
async fn blocking_returns_the_work_s_value() {
    assert_eq!(blocking("add", || 2 + 2).await, Ok(4));
}

#[tokio::test]
#[expect(clippy::disallowed_methods, reason = "a test of `blocking` itself")]
async fn blocking_turns_a_panic_into_panicked_with_its_message() {
    let failed = blocking("read the journal", || -> u8 { panic!("journal is empty") })
        .await
        .expect_err("a panicking closure gives no value");
    assert_eq!(
        failed,
        TaskFailed::Panicked(Bug {
            operation: "read the journal",
            message: "journal is empty".to_string(),
        })
    );
    assert_eq!(failed.to_string(), "read the journal panicked: journal is empty");
}

#[tokio::test]
async fn joined_turns_a_cancelled_task_into_shutdown() {
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let failed = joined("wait forever", task).await.expect_err("an aborted task gives no value");
    assert_eq!(
        failed,
        TaskFailed::Shutdown(Shutdown {
            operation: "wait forever",
            outcome: ShutdownOutcome::Unknown,
        })
    );
    assert!(!failed.is_panic());
}

#[test]
#[expect(clippy::disallowed_methods, reason = "a test of `blocking` itself")]
fn blocking_sent_after_its_runtime_shut_down_did_not_run() {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let handle = runtime.handle().clone();
    drop(runtime);
    let failed = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let _entered = handle.enter();
            blocking("read the tags", || 1).await
        })
        .expect_err("the pool runs nothing once it shut down");
    assert_eq!(
        failed,
        TaskFailed::Shutdown(Shutdown {
            operation: "read the tags",
            outcome: ShutdownOutcome::NotRun,
        })
    );
    assert_eq!(failed.to_string(), "read the tags did not run: its runtime is shutting down");
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
        TaskFailed::Panicked(Bug {
            operation: "fill the read-ahead",
            message: "no window 7".to_string(),
        })
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
    assert_eq!(failed.operation, "apply one slot");
    assert_eq!(failed.message, "row count went negative");
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
        Err(Bug { operation: "decode", message: "decoder state is wrong".to_string() })
    );
}

#[test]
fn caught_sync_turns_a_panic_into_its_bug() {
    let failed =
        caught_sync("read callback", || -> i32 { panic!("buffer gone") }).expect_err("panicked");
    assert_eq!(failed, Bug { operation: "read callback", message: "buffer gone".to_string() });
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
        Err(Bug {
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
        Err(TaskFailed::Panicked(Bug {
            operation: "upload a file",
            message: "a panic whose payload is not text".to_string(),
        }))
    );
}

#[tokio::test]
async fn an_owned_task_s_panic_goes_to_the_sink_then_its_owner_ends_its_work() {
    let bugs = HeardBugs::shared();
    let (ended, owner_heard) = tokio::sync::oneshot::channel();
    let task = spawn_owned(
        "upload files",
        bugs.clone(),
        async { panic!("the queue's first row is gone") },
        |bug| async move {
            ended.send(bug).unwrap();
        },
    );
    task.ended().await;
    let bug = Bug { operation: "upload files", message: "the queue's first row is gone".into() };
    assert_eq!(bugs.take(), std::slice::from_ref(&bug));
    assert_eq!(owner_heard.await.unwrap(), bug);
}

#[tokio::test]
async fn an_owned_task_s_ending_that_panics_goes_to_the_sink_too() {
    let bugs = HeardBugs::shared();
    spawn_owned("sync", bugs.clone(), async { panic!("a pass panicked") }, |_| async {
        panic!("recording the failure panicked")
    })
    .ended()
    .await;
    let messages: Vec<_> = bugs.take().into_iter().map(|bug| bug.message).collect();
    assert_eq!(messages, ["a pass panicked", "recording the failure panicked"]);
}

#[tokio::test]
async fn an_owned_task_that_finishes_reports_nothing() {
    let bugs = HeardBugs::shared();
    spawn_owned("follow the network", bugs.clone(), async {}, |_| async {
        unreachable!("only a panic ends the work")
    })
    .ended()
    .await;
    assert!(bugs.take().is_empty());
}

#[tokio::test]
async fn a_completing_task_s_panic_goes_to_its_waiting_caller_alone() {
    let bugs = HeardBugs::shared();
    let failed = spawn_completing("close the store", bugs.clone(), async {
        panic!("a writer is still held")
    })
    .await;
    assert_eq!(
        failed,
        Err::<Result<(), Bug>, _>(TaskFailed::Panicked(Bug {
            operation: "close the store",
            message: "a writer is still held".into(),
        }))
    );
    assert!(bugs.take().is_empty());
    assert_eq!(spawn_completing("add", bugs, async { Ok::<_, Bug>(2 + 2) }).await, Ok(Ok(4)));
}

/// Sends each bug it hears of, so a test waits for the report.
struct SentBugs(tokio::sync::mpsc::UnboundedSender<Bug>);

impl BugSink for SentBugs {
    fn report(&self, bug: &Bug) {
        self.0.send(bug.clone()).unwrap();
    }
}

/// Work whose state is gone: it panics before it can answer.
fn loop_is_gone() -> Result<(), Bug> {
    panic!("the loop is gone")
}

#[tokio::test]
async fn a_completing_task_whose_caller_stopped_waiting_reports_its_panic() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let (go, wait) = tokio::sync::oneshot::channel::<()>();
    let completion = spawn_completing("stop sync", Arc::new(SentBugs(sent)), async move {
        wait.await.unwrap();
        loop_is_gone()
    });
    drop(completion);
    go.send(()).unwrap();
    assert_eq!(
        reported.recv().await,
        Some(Bug { operation: "stop sync", message: "the loop is gone".into() })
    );
}

#[tokio::test]
async fn a_completing_task_whose_caller_stopped_waiting_reports_the_bug_its_error_carries() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let (go, wait) = tokio::sync::oneshot::channel::<()>();
    let completion = spawn_completing("close custody", Arc::new(SentBugs(sent)), async move {
        wait.await.unwrap();
        Err::<u8, _>(TaskFailed::Panicked(Bug {
            operation: "forget the keys",
            message: "no key session".into(),
        }))
    });
    drop(completion);
    go.send(()).unwrap();
    assert_eq!(
        reported.recv().await,
        Some(Bug { operation: "forget the keys", message: "no key session".into() })
    );
}

#[tokio::test]
async fn blocking_work_whose_caller_stopped_waiting_reports_its_panic_and_carried_bug() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let bugs: BugSinkRef = Arc::new(SentBugs(sent));
    let (go, wait) = std::sync::mpsc::channel::<()>();
    let completion = blocking_completing("read the tags", bugs.clone(), move || {
        wait.recv().unwrap();
        loop_is_gone()
    });
    drop(completion);
    go.send(()).unwrap();
    assert_eq!(
        reported.recv().await,
        Some(Bug { operation: "read the tags", message: "the loop is gone".into() })
    );
    let carried = Bug { operation: "seal a part", message: "no key".into() };
    let (go, wait) = std::sync::mpsc::channel::<()>();
    let error = carried.clone();
    let completion = blocking_completing("write the part", bugs, move || -> Result<(), _> {
        wait.recv().unwrap();
        Err(std::io::Error::other(error))
    });
    drop(completion);
    go.send(()).unwrap();
    assert_eq!(reported.recv().await, Some(carried));
}

#[tokio::test]
async fn blocking_work_answers_a_caller_still_waiting() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let bugs: BugSinkRef = Arc::new(SentBugs(sent));
    assert_eq!(blocking_completing("add", bugs.clone(), || Ok::<_, Bug>(2 + 2)).await, Ok(Ok(4)));
    assert_eq!(
        blocking_completing("read the tags", bugs, loop_is_gone).await,
        Err(TaskFailed::Panicked(Bug {
            operation: "read the tags",
            message: "the loop is gone".into()
        }))
    );
    assert!(reported.try_recv().is_err(), "a caller heard the bug, so no sink did");
}

/// Work that answered before its caller stopped waiting, its answer still
/// unread, reports that answer's bug as the caller drops it: no one else
/// will read it.
#[tokio::test]
async fn a_completing_task_dropped_with_its_answer_unread_reports_the_answer_s_bug() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let (answering, answered) = tokio::sync::oneshot::channel();
    let completion = spawn_completing("stop sync", Arc::new(SentBugs(sent)), async move {
        answering.send(()).unwrap();
        loop_is_gone()
    });
    // The work answers in the same poll that tells, and on this one-thread
    // runtime the test runs again only after that poll: the answer is in
    // hand once the test hears.
    crate::clock::within(answered).await.unwrap().unwrap();
    drop(completion);
    assert_eq!(
        reported.try_recv(),
        Ok(Bug { operation: "stop sync", message: "the loop is gone".into() })
    );
}

/// A test's sink dropped with a bug no test took fails the test, so a bug no
/// test expected never passes unseen.
#[test]
#[should_panic(expected = "coven reported bugs no test took")]
fn a_test_sink_dropped_with_a_bug_untaken_fails_the_test() {
    let bugs = HeardBugs::shared();
    report(bugs.as_ref(), &Bug { operation: "read the tags", message: "no tags".into() });
}
