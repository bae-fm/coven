use std::sync::Arc;

use super::*;

/// Panics with `message`. Its `panic!` is on line [`PANIC_LINE`], column 5.
const PANIC_LINE: u32 = line!() + 2;
fn panics(message: &str) -> ! {
    panic!("{message}")
}

/// Where [`panics`] panics.
fn panic_site() -> Option<SourceLocation> {
    Some(SourceLocation { file: file!().to_string(), line: PANIC_LINE, column: 5 })
}

/// The bug `operation` gives once [`panics`] panics in it with `message`.
fn panicked(operation: &'static str, message: &str) -> Bug {
    Bug { operation, message: message.to_string(), location: panic_site(), kind: BugKind::Panic }
}

#[tokio::test]
#[expect(clippy::disallowed_methods, reason = "a test of `blocking` itself")]
async fn blocking_returns_the_work_s_value() {
    assert_eq!(blocking("add", || 2 + 2).await, Ok(4));
}

#[tokio::test]
#[expect(clippy::disallowed_methods, reason = "a test of `blocking` itself")]
async fn blocking_turns_a_panic_into_panicked_with_its_message_and_place() {
    let failed = blocking("read the journal", || -> u8 { panics("journal is empty") })
        .await
        .expect_err("a panicking closure gives no value");
    assert_eq!(failed, TaskFailed::Panicked(panicked("read the journal", "journal is empty")));
    assert_eq!(failed.to_string(), "read the journal panicked: journal is empty");
}

#[tokio::test]
async fn a_cancelled_task_s_join_is_shutdown() {
    let task = tokio::spawn(caught("wait forever", std::future::pending::<()>()));
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

/// A task whose work didn't run under a catch of coven's panics into its
/// join, where no catch saw the panic begin: its bug keeps the message and
/// names no place.
#[tokio::test]
async fn a_panic_that_reached_its_join_uncaught_names_no_place() {
    let task = tokio::spawn(async {
        let index = 7;
        panic!("no window {index}");
    });
    let failed = TaskFailed::of_join("fill the read-ahead", task.await.expect_err("panicked"));
    assert_eq!(
        failed,
        TaskFailed::Panicked(Bug {
            operation: "fill the read-ahead",
            message: "no window 7".to_string(),
            location: None,
            kind: BugKind::Panic,
        })
    );
}

/// A task whose work runs under `caught` hands its panic, and where it began,
/// back through its join as a value.
#[tokio::test]
async fn a_task_s_caught_panic_names_its_place_through_the_join() {
    let task = tokio::spawn(caught("fill the read-ahead", async {
        tokio::task::yield_now().await;
        panics("no window 7")
    }));
    let failed = joined("fill the read-ahead", task).await;
    assert_eq!(failed, Err(TaskFailed::Panicked(panicked("fill the read-ahead", "no window 7"))));
}
/// A panic payload whose own drop panics, as `std::panic::panic_any` can
/// throw.
struct PanicsWhenDropped;

impl Drop for PanicsWhenDropped {
    fn drop(&mut self) {
        panic!("the payload's drop panicked");
    }
}

#[tokio::test]
async fn a_joined_payload_whose_drop_panics_does_not_unwind() {
    let task = tokio::spawn(async { std::panic::panic_any(PanicsWhenDropped) });
    let failed = TaskFailed::of_join("upload a file", task.await.expect_err("panicked"));
    assert_eq!(
        failed,
        TaskFailed::Panicked(Bug {
            operation: "upload a file",
            message: "a panic whose payload is not text".to_string(),
            location: None,
            kind: BugKind::Panic,
        })
    );
}

#[tokio::test]
async fn an_owned_task_s_panic_goes_to_the_sink_then_its_owner_ends_its_work() {
    let bugs = HeardBugs::shared();
    let (ended, owner_heard) = tokio::sync::oneshot::channel();
    let task = spawn_owned(
        "upload files",
        bugs.clone(),
        async { panics("the queue's first row is gone") },
        |bug| async move {
            ended.send(bug).unwrap();
        },
    );
    task.ended().await;
    let bug = panicked("upload files", "the queue's first row is gone");
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
        panics("a writer is still held")
    })
    .await;
    assert_eq!(
        failed,
        Err::<Result<(), Bug>, _>(TaskFailed::Panicked(panicked(
            "close the store",
            "a writer is still held"
        )))
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
    panics("the loop is gone")
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
    assert_eq!(reported.recv().await, Some(panicked("stop sync", "the loop is gone")));
}

#[tokio::test]
async fn a_completing_task_whose_caller_stopped_waiting_reports_the_bug_its_error_carries() {
    let (sent, mut reported) = tokio::sync::mpsc::unbounded_channel();
    let (go, wait) = tokio::sync::oneshot::channel::<()>();
    let completion = spawn_completing("close custody", Arc::new(SentBugs(sent)), async move {
        wait.await.unwrap();
        Err::<u8, _>(TaskFailed::Panicked(Bug::new("forget the keys", "no key session")))
    });
    drop(completion);
    go.send(()).unwrap();
    let carried = reported.recv().await.expect("the sink heard the carried bug");
    assert_eq!(
        (carried.operation, carried.message.as_str()),
        ("forget the keys", "no key session")
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
    assert_eq!(reported.recv().await, Some(panicked("read the tags", "the loop is gone")));
    let carried = Bug::new("seal a part", "no key");
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
        Err(TaskFailed::Panicked(panicked("read the tags", "the loop is gone")))
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
    assert_eq!(reported.try_recv(), Ok(panicked("stop sync", "the loop is gone")));
}

/// A test's sink dropped with a bug no test took fails the test, so a bug no
/// test expected never passes unseen.
#[test]
#[should_panic(expected = "coven reported bugs no test took")]
fn a_test_sink_dropped_with_a_bug_untaken_fails_the_test() {
    let bugs = HeardBugs::shared();
    report(bugs.as_ref(), &Bug::new("read the tags", "no tags"));
}

#[tokio::test]
async fn the_hang_report_names_running_operations_and_the_latest_to_finish() {
    let (release, held) = std::sync::mpsc::channel::<()>();
    let (started, running) = tokio::sync::oneshot::channel();
    let bugs: BugSinkRef = std::sync::Arc::new(LoggedBugs);
    let work = blocking_completing("hold the report test's thread", bugs.clone(), move || {
        operation_step("waiting for the test's release");
        started.send(()).unwrap();
        held.recv().unwrap();
        Ok::<_, Bug>(())
    });
    running.await.unwrap();
    let during = in_flight::report();
    assert!(
        during.contains(
            "hold the report test's thread: running on the blocking pool, at: waiting for the test's release"
        ),
        "{during}"
    );
    release.send(()).unwrap();
    work.await.unwrap().unwrap();
    spawn_completing("finish the report test's task", bugs, async { Ok::<_, Bug>(()) })
        .await
        .unwrap()
        .unwrap();
    let after = in_flight::report();
    assert!(!after.contains("hold the report test's thread: running"), "{after}");
    assert!(after.contains("hold the report test's thread\n"), "{after}");
    assert!(after.contains("finish the report test's task"), "{after}");
}
