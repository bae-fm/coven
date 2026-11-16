use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_lite::future::{block_on, yield_now};

use super::*;

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

/// One panic the test binary's hook ran for.
struct Heard {
    message: String,
    /// The operation of the innermost catch of this crate's on the panicking
    /// thread when the hook ran.
    operation: Option<&'static str>,
}

static HEARD: std::sync::Mutex<Vec<Heard>> = std::sync::Mutex::new(Vec::new());

static HOOKED: std::sync::Once = std::sync::Once::new();

/// Install the test binary's hook, once. It records each panic in [`HEARD`]
/// and then runs the hook set before it. This crate's hook goes in before the
/// first catch, in front of this one, so the first catch must not run before
/// this: replacing a hook leaves a moment in which another test's panic finds
/// neither hook. Each test that catches or begins a panic calls this first.
fn with_test_hook() {
    HOOKED.call_once(|| {
        let earlier = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic| {
            locks::lock_unpoisoned(&HEARD).push(Heard {
                message: panic_message(panic.payload()),
                operation: catching_operation(),
            });
            earlier(panic);
        }));
    });
}

/// The operation of the innermost catch of this crate's on the panicking
/// thread, for each panic the test hook ran for with `message`.
fn operations_heard(message: &str) -> Vec<Option<&'static str>> {
    let heard = locks::lock_unpoisoned(&HEARD);
    heard.iter().filter(|heard| heard.message == message).map(|heard| heard.operation).collect()
}

/// Sets its flag when dropped: the cleanup a caller's work holds.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn caught_fails_the_work_and_its_held_cleanup_runs() {
    with_test_hook();
    let cleaned = Arc::new(AtomicBool::new(false));
    let guard = SetOnDrop(cleaned.clone());
    let failed = block_on(caught("apply one slot", async move {
        let _guard = guard;
        yield_now().await;
        panic!("row count went negative");
    }))
    .expect_err("the work panicked");
    assert_eq!(failed.operation, "apply one slot");
    assert_eq!(failed.message, "row count went negative");
    assert!(cleaned.load(Ordering::SeqCst), "the work's drop guard ran");
}

#[test]
fn caught_names_where_a_later_poll_panicked() {
    with_test_hook();
    let failed = block_on(caught("apply one slot", async {
        yield_now().await;
        panics("row count went negative")
    }));
    assert_eq!(failed, Err(panicked("apply one slot", "row count went negative")));
}

#[test]
fn caught_returns_the_work_s_value_across_polls() {
    with_test_hook();
    let value = block_on(caught("count", async {
        yield_now().await;
        3
    }));
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

#[test]
fn caught_keeps_a_panicking_drop_of_the_failed_work_from_unwinding() {
    with_test_hook();
    let failed = block_on(caught("decode", PanicsTwice));
    assert_eq!(failed, Err(panicked("decode", "decoder state is wrong")));
}

#[test]
fn caught_sync_turns_a_panic_into_its_bug() {
    with_test_hook();
    let failed =
        caught_sync("read callback", || -> i32 { panics("buffer gone") }).expect_err("panicked");
    assert_eq!(failed, panicked("read callback", "buffer gone"));
    assert_eq!(caught_sync("read callback", || 5), Ok(5));
}

/// A catch inside another names its own panic, and the outer catch, once
/// the inner one has ended, names the panic that reached it.
#[test]
fn a_catch_inside_another_names_its_own_panic() {
    with_test_hook();
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
/// leaves this thread's catch naming its own: each thread's catch records only
/// the panics that begin on its own thread.
#[test]
fn a_panic_on_another_thread_leaves_this_catch_s_place() {
    with_test_hook();
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
    with_test_hook();
    assert_eq!(catching_operation(), None);
    let outer = caught_sync("serve a message", || {
        let inner = caught_sync("decode", catching_operation);
        assert_eq!(inner, Ok(Some("decode")));
        catching_operation()
    });
    assert_eq!(outer, Ok(Some("serve a message")));
    assert_eq!(catching_operation(), None);
}

/// A panic that work handles itself inside a catch, as an async runtime does
/// for a task it runs on the catch's thread, still finds that catch's
/// operation as the hook runs.
#[test]
fn a_panic_handled_inside_a_catch_names_the_catch_s_operation() {
    with_test_hook();
    let served = caught_sync("serve a message", || {
        std::panic::catch_unwind(|| -> u8 { panics("a handled panic names its catch") })
            .unwrap_or(0)
    });
    assert_eq!(served, Ok(0));
    assert_eq!(operations_heard("a handled panic names its catch"), [Some("serve a message")]);
}

/// This crate's hook runs before the hook the app set, and the app's hook
/// still runs for every panic.
#[test]
fn the_app_s_own_panic_hook_still_runs() {
    with_test_hook();
    let failed = caught_sync("read callback", || -> i32 { panics("the app's hook hears this") });
    assert_eq!(failed, Err(panicked("read callback", "the app's hook hears this")));
    assert_eq!(
        operations_heard("the app's hook hears this"),
        [Some("read callback")],
        "the app's hook ran for the panic"
    );
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
        "release reserved file names found a bug: 2 released names were not reserved"
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
    with_test_hook();
    let failed =
        caught_sync("seek callback", || -> i32 { std::panic::panic_any(PanicsWhenDropped) })
            .expect_err("panicked");
    assert_eq!(failed.operation, "seek callback");
    assert_eq!(failed.message, "a panic whose payload is not text");
}

#[test]
fn an_unlocated_bug_keeps_the_message_and_names_no_place() {
    with_test_hook();
    let bug = bug_of_unlocated("fill the read-ahead", Box::new("no window 7"));
    assert_eq!(
        bug,
        Bug {
            operation: "fill the read-ahead",
            message: "no window 7".to_string(),
            location: None,
            kind: BugKind::Panic,
        }
    );
    let dropping = bug_of_unlocated("upload a file", Box::new(PanicsWhenDropped));
    assert_eq!(dropping.message, "a panic whose payload is not text");
}
