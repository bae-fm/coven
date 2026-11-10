use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::*;

/// Panics with `message`. Its `panic!` is on line [`PANIC_LINE`], column 5.
const PANIC_LINE: u32 = line!() + 2;
fn panics(message: &str) -> ! {
    panic!("{message}")
}

/// Panics with `message` from a second place, on line [`PANIC_AGAIN_LINE`].
const PANIC_AGAIN_LINE: u32 = line!() + 2;
fn panics_again(message: &str) -> ! {
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
async fn caught_names_where_a_later_poll_panicked() {
    let failed = caught("apply one slot", async {
        tokio::task::yield_now().await;
        panics("row count went negative")
    })
    .await;
    assert_eq!(failed, Err(panicked("apply one slot", "row count went negative")));
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
        panics("decoder state is wrong")
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
    assert_eq!(failed, Err(panicked("decode", "decoder state is wrong")));
}

#[test]
fn caught_sync_turns_a_panic_into_its_bug() {
    let failed =
        caught_sync("read callback", || -> i32 { panics("buffer gone") }).expect_err("panicked");
    assert_eq!(failed, panicked("read callback", "buffer gone"));
    assert_eq!(caught_sync("read callback", || 5), Ok(5));
}

/// A catch inside another names its own panic, and the outer catch, once
/// the inner one has ended, names the panic that reached it.
#[test]
fn a_catch_inside_another_names_its_own_panic() {
    let outer = caught_sync("serve a message", || {
        let inner = caught_sync("decode", || -> u8 { panics("decoder state is wrong") });
        assert_eq!(inner, Err(panicked("decode", "decoder state is wrong")));
        panics_again("the reply channel is gone")
    })
    .map(|_: ()| ());
    assert_eq!(
        outer,
        Err(Bug {
            operation: "serve a message",
            message: "the reply channel is gone".to_string(),
            location: Some(SourceLocation {
                file: file!().to_string(),
                line: PANIC_AGAIN_LINE,
                column: 5,
            }),
            kind: BugKind::Panic,
        })
    );
    let recovered = caught_sync("serve a message", || {
        caught_sync("decode", || -> u8 { panics("decoder state is wrong") }).unwrap_or(0)
    });
    assert_eq!(recovered, Ok(0));
}

/// Panics on a thread of its own as it drops, and waits for that thread.
struct PanicsElsewhereWhenDropped;

impl Drop for PanicsElsewhereWhenDropped {
    fn drop(&mut self) {
        let other = std::thread::spawn(|| panics_again("another thread's bug"));
        assert!(other.join().is_err(), "the other thread panicked");
    }
}

/// A panic on another thread, while this thread's panic unwinds to its catch,
/// leaves this thread's catch naming its own: each thread's catch hears only
/// its own thread's panics.
#[test]
fn a_panic_on_another_thread_leaves_this_catch_s_place() {
    let failed = caught_sync("read callback", || -> i32 {
        let _elsewhere = PanicsElsewhereWhenDropped;
        panics("buffer gone")
    });
    assert_eq!(failed, Err(panicked("read callback", "buffer gone")));
}

/// An app's own hook, run as a panic begins, reads the operation of the
/// innermost catch on the panicking thread: the inner one while it runs, the
/// outer one once it has ended, and none outside every catch.
#[test]
fn a_hook_names_the_innermost_catch_s_operation() {
    assert_eq!(catching_operation(), None);
    let outer = caught_sync("serve a message", || {
        let inner = caught_sync("decode", catching_operation);
        assert_eq!(inner, Ok(Some("decode")));
        catching_operation()
    });
    assert_eq!(outer, Ok(Some("serve a message")));
    assert_eq!(catching_operation(), None);
}

/// A panic that work handles itself inside a catch, as tokio does for a
/// task it runs on the catch's thread, still finds that catch's operation as
/// the hook runs.
#[test]
fn a_panic_handled_inside_a_catch_names_the_catch_s_operation() {
    static NAMED: std::sync::Mutex<Vec<Option<&'static str>>> = std::sync::Mutex::new(Vec::new());
    let earlier = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        if panic_message(panic.payload()) == "a handled panic names its catch" {
            NAMED.lock().unwrap().push(catching_operation());
        }
        earlier(panic);
    }));
    let served = caught_sync("serve a message", || {
        std::panic::catch_unwind(|| -> u8 { panics("a handled panic names its catch") })
            .unwrap_or(0)
    });
    assert_eq!(served, Ok(0));
    assert_eq!(*NAMED.lock().unwrap(), [Some("serve a message")]);
}

/// coven's hook runs in front of the one the app set, which still hears
/// every panic.
#[test]
fn the_app_s_own_panic_hook_still_runs() {
    static HEARD: AtomicBool = AtomicBool::new(false);
    let earlier = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic| {
        if panic_message(panic.payload()) == "the app's hook hears this" {
            HEARD.store(true, Ordering::SeqCst);
        }
        earlier(panic);
    }));
    let failed = caught_sync("read callback", || -> i32 { panics("the app's hook hears this") });
    assert_eq!(failed, Err(panicked("read callback", "the app's hook hears this")));
    assert!(HEARD.load(Ordering::SeqCst), "the app's hook heard the panic");
}

/// A bug code finds without a panic names the line that made it.
#[test]
fn a_bug_made_without_a_panic_names_where_it_was_made() {
    let bug = Bug::new("release reserved file names", "2 released names were not reserved");
    let made_on = line!() - 1;
    assert_eq!(
        bug.location,
        Some(SourceLocation { file: file!().to_string(), line: made_on, column: 15 })
    );
    assert_eq!(bug.kind, BugKind::Found);
    assert_eq!(
        bug.to_string(),
        "release reserved file names panicked: 2 released names were not reserved"
    );
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
        caught_sync("seek callback", || -> i32 { std::panic::panic_any(PanicsWhenDropped) })
            .expect_err("panicked");
    assert_eq!(failed.operation, "seek callback");
    assert_eq!(failed.message, "a panic whose payload is not text");
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
