//! Where a panic coven catches began (api.md E15).
//!
//! A panic's payload holds only its text: `std::panic::Location` reaches the
//! panic hook alone. So each of coven's catches marks its thread as catching
//! while its work runs, and a hook coven adds in front of the process's own
//! writes where a panic began into the innermost mark on the panicking
//! thread. The catch takes it back with the payload, so the location is part
//! of what the catch returns. Work run as its own task or on the blocking
//! pool runs under such a catch on its own thread, and its bug, location
//! included, travels through the join as a value.
//!
//! The hook is the one piece of state the process shares, and it is safe with
//! many threads: it is added once, chained in front of whatever hook was set
//! before, which it calls in turn, and it touches only the panicking thread's
//! own mark. A panic unwinds only on the thread that raised it, so the mark it
//! writes is the one the catch around it reads. A panic outside every catch
//! of coven's finds no mark and is left to the hooks after coven's.

use std::any::Any;
use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe, Location, PanicHookInfo};
use std::sync::Once;

/// Where in the code a bug showed itself: the file, line and column of the
/// panic, or of the code that found the bug and made it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceLocation {
    /// The source file, as the compiler named it.
    pub file: String,
    /// The line in it, from 1.
    pub line: u32,
    /// The column in that line, from 1.
    pub column: u32,
}

impl SourceLocation {
    /// The place `location` names.
    pub(crate) fn of(location: &Location<'_>) -> Self {
        Self { file: location.file().to_string(), line: location.line(), column: location.column() }
    }
}

impl std::fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

/// What a thread's innermost catch of coven's has seen.
enum Mark {
    /// No catch of coven's is running on this thread.
    Unmarked,
    /// One is, running `operation`, and `location` is where the latest panic
    /// inside it began, once one has.
    Catching { operation: &'static str, location: Option<SourceLocation> },
}

thread_local! {
    /// This thread's innermost catch of coven's, which the hook writes and
    /// the catch reads back. A catch inside another keeps the outer one's
    /// mark aside and puts it back as it ends.
    static MARK: Cell<Mark> = const { Cell::new(Mark::Unmarked) };
}

/// Adds coven's hook in front of the process's, once.
static CHAINED: Once = Once::new();

/// `work`'s value, or the payload of its panic and where that panic began.
/// While `work` runs, this catch is the thread's innermost, named
/// `operation`.
///
/// The location is `None` only for an unwind coven's hook didn't see begin:
/// one resumed with `std::panic::resume_unwind` from a panic elsewhere, or a
/// panic after the app replaced the process's hook with one that doesn't call
/// the hook it replaced.
pub(crate) fn catch_located<R>(
    operation: &'static str,
    work: impl FnOnce() -> R,
) -> Result<R, (Box<dyn Any + Send>, Option<SourceLocation>)> {
    chain_hook();
    let outer = swap_mark(Mark::Catching { operation, location: None });
    let caught = catch_unwind(AssertUnwindSafe(work));
    // `outer` is `None` once the thread's locals are gone, as its last
    // destructors run: then there is no mark to restore or read.
    let inner = outer.and_then(swap_mark);
    caught.map_err(|payload| match inner {
        Some(Mark::Catching { location, .. }) => (payload, location),
        Some(Mark::Unmarked) | None => (payload, None),
    })
}

/// The operation of the innermost catch of coven's running on this thread,
/// or `None` outside every one: for the app's own panic hook, to say what
/// the panicking work was doing. A panic a catch inside that work handles
/// itself, such as a task tokio runs on this thread and catches, still
/// names it. It never panics, so a hook can call it: a panic inside a hook
/// ends the process.
pub fn catching_operation() -> Option<&'static str> {
    MARK.try_with(|place| {
        let mark = place.replace(Mark::Unmarked);
        let operation = match &mark {
            Mark::Catching { operation, .. } => Some(*operation),
            Mark::Unmarked => None,
        };
        place.set(mark);
        operation
    })
    .ok()
    .flatten()
}

/// Put `mark` in this thread's place and return what was there, or `None`
/// when the thread's locals are already gone.
fn swap_mark(mark: Mark) -> Option<Mark> {
    MARK.try_with(|place| place.replace(mark)).ok()
}

/// Add coven's hook in front of the process's, once. A thread that is
/// panicking can't set a hook, and std would panic again inside the unwind,
/// which ends the process, so a catch entered during an unwind leaves it to
/// the next catch.
fn chain_hook() {
    if std::thread::panicking() {
        return;
    }
    CHAINED.call_once(|| {
        let earlier = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic| {
            mark_panic(panic);
            earlier(panic);
        }));
    });
}

/// Write where `panic` began into this thread's innermost catch, if one of
/// coven's is running. A panic inside the hook ends the process, so nothing
/// here can panic: the mark is a `Cell`, taken and put back whole.
fn mark_panic(panic: &PanicHookInfo<'_>) {
    let Some(location) = panic.location() else {
        return;
    };
    #[expect(
        clippy::let_underscore_must_use,
        reason = "a thread whose locals are gone is exiting, and runs no catch of coven's"
    )]
    let _ = MARK.try_with(|place| {
        if let Mark::Catching { operation, .. } = place.replace(Mark::Unmarked) {
            place.set(Mark::Catching { operation, location: Some(SourceLocation::of(location)) });
        }
    });
}
