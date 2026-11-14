//! What a test's hung process was doing, for tests only.
//!
//! A test that stops making progress prints nothing but nextest's timeout.
//! Every blocking or task-run operation coven starts goes through
//! `blocking_completing` or `spawn_completing`, so these record each one by
//! name while it runs, and the last few that finished. A thread started at
//! the first operation prints both once, `HANG_GUARD` after it, which is
//! before nextest ends the test: an operation still running then is the
//! stuck one (its state says whether the pool ever started it), and none
//! running means the test waits on an event in its own async code.

use crate::locks::lock_unpoisoned;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{Mutex, Once};

/// How many finished operations the report keeps.
const RECENT: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// Handed to the blocking pool, not yet started by a thread of it.
    Queued,
    /// A blocking closure a pool thread is running.
    Running,
    /// A task on the runtime, polled until it finishes.
    Task,
}

struct Flights {
    next: u64,
    /// Each running operation's name, state and the step it last named.
    running: BTreeMap<u64, (&'static str, State, Option<&'static str>)>,
    /// The latest finished operations, oldest first, by their number.
    recent: Vec<(u64, &'static str)>,
}

thread_local! {
    /// The operation whose closure this pool thread is running.
    static RUNNING_HERE: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Name the step the blocking operation running on this thread has reached,
/// so a hang report says where inside it the operation waits.
pub(crate) fn step(name: &'static str) {
    let Some(id) = RUNNING_HERE.with(Cell::get) else { return };
    with_flights(|flights| {
        if let Some(entry) = flights.running.get_mut(&id) {
            entry.2 = Some(name);
        }
    });
}

static FLIGHTS: Mutex<Option<Flights>> = Mutex::new(None);
static WATCHDOG: Once = Once::new();

fn with_flights<R>(use_it: impl FnOnce(&mut Flights) -> R) -> R {
    let mut slot = lock_unpoisoned(&FLIGHTS);
    use_it(slot.get_or_insert_with(|| Flights {
        next: 0,
        running: BTreeMap::new(),
        recent: Vec::new(),
    }))
}

/// One operation in the record while it lives; dropping it, as the
/// operation ends or its runtime drops it, moves it to the recent ones.
pub(crate) struct Flight(u64);

impl Flight {
    pub(crate) fn begin(operation: &'static str, state: State) -> Self {
        WATCHDOG.call_once(|| {
            // A process that cannot start the thread simply reports nothing.
            #[expect(
                clippy::let_underscore_must_use,
                reason = "the report is a courtesy of a test process"
            )]
            let _ = std::thread::Builder::new().name("coven hang report".into()).spawn(|| {
                crate::clock::wait_out_hang_guard();
                eprintln!("{}", report());
            });
        });
        with_flights(|flights| {
            flights.next += 1;
            let id = flights.next;
            flights.running.insert(id, (operation, state, None));
            Self(id)
        })
    }

    /// A pool thread has started the closure.
    pub(crate) fn started(&self) {
        with_flights(|flights| {
            if let Some(entry) = flights.running.get_mut(&self.0) {
                entry.1 = State::Running;
            }
        });
        RUNNING_HERE.with(|here| here.set(Some(self.0)));
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        RUNNING_HERE.with(|here| {
            if here.get() == Some(self.0) {
                here.set(None);
            }
        });
        with_flights(|flights| {
            if let Some((operation, _, _)) = flights.running.remove(&self.0) {
                flights.recent.push((self.0, operation));
                if flights.recent.len() > RECENT {
                    flights.recent.remove(0);
                }
            }
        });
    }
}

/// The operations running and the latest to finish, each by the order the
/// process started it in.
pub(crate) fn report() -> String {
    with_flights(|flights| {
        let mut text = format!(
            "coven operations, {} started, {} running:",
            flights.next,
            flights.running.len()
        );
        for (number, (operation, state, step)) in &flights.running {
            let state = match state {
                State::Queued => "queued on the blocking pool, never started",
                State::Running => "running on the blocking pool",
                State::Task => "a task on the runtime",
            };
            text.push_str(&format!("\n  #{number} {operation}: {state}"));
            if let Some(step) = step {
                text.push_str(&format!(", at: {step}"));
            }
        }
        text.push_str("\nthe latest to finish:");
        for (number, operation) in &flights.recent {
            text.push_str(&format!("\n  #{number} {operation}"));
        }
        text
    })
}
