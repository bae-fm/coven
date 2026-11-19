// The crate the hand-out rules check under the policy of this folder: each
// line here and in database.rs that holds a finding of
// `clippy::owner_hands_out_dependency` holds a `finding` comment, and
// handles.rs and shared.rs mark the other two rules' findings.

pub mod database;
pub mod handles;
pub mod shared;

use std::sync::Arc;

use handles::State;

use dependencies_base::Records;
use rusqlite::Connection;

#[derive(Clone)]
pub struct Clock;

impl Clock {
    pub fn new() -> Clock {
        Clock
    }
}

pub struct Database;

#[derive(Clone)]
pub struct Permit;

async fn ready() {}

fn forward(construct: impl FnOnce(Clock)) {
    construct(Clock::new())
}

// An owner hands out neither a capability nor an owner it holds. A listed
// root may return the graph it builds.
pub struct Child {
    clock: Clock,
}

pub struct Owner {
    child: Child,
}

impl Owner {
    fn child(/* finding */ &self) -> &Child {
        &self.child
    }

    fn clock(/* finding */ &self) -> Clock {
        self.child.clock.clone()
    }

    pub fn open(&self) -> Child {
        Child { clock: self.child.clock.clone() }
    }
}

// A public method of an owner returns no other owner.
pub struct Maker {
    clock: Clock,
}

impl Maker {
    pub fn child(/* finding */ &self) -> Child {
        Child { clock: Clock::new() }
    }
}

// A public field of an owner, or one that holds an owner, hands it out.
pub struct Exposed {
    pub clock: Clock, // finding
    pub count: usize, // finding
}

pub struct Transfer {
    child: Child,
}

pub struct Envelope {
    pub transfer: Transfer, // finding
    pub count: usize,
}

// A closed session at the crate's root hands its internal dependencies to
// every module.
pub struct Batch {
    connection: Database, // finding
}

// A value of an earlier crate's type whose methods hand out the raw handle
// it holds hands that out too, as does a value that holds it.
pub struct Wrapper {
    records: Records,
}

impl Wrapper {
    fn records(/* finding */ &self) -> &Records {
        &self.records
    }
}

pub struct Outer {
    wrapper: Wrapper,
}

impl Outer {
    fn wrapper(/* finding */ &self) -> &Wrapper {
        &self.wrapper
    }

    fn fresh(&self) -> Wrapper {
        Wrapper { records: Records::open() }
    }
}

// A method that takes its owner mutably takes no raw handle.
pub struct Cache {
    count: usize,
}

impl Cache {
    fn absorb(/* finding */ &mut self, connection: &Connection) {
        self.count = connection.changes();
    }

    fn read(&self, connection: &Connection) -> usize {
        connection.changes() + self.count
    }
}

// A task taken whole or mutably hands on a task it holds; one that lends
// it still has it.
pub struct Plan {
    permit: Permit,
}

impl Plan {
    fn into_permit(self) -> Permit {
        self.permit
    }

    fn next(&mut self) -> Permit {
        std::mem::replace(&mut self.permit, Permit)
    }

    fn copy(/* finding */ &self) -> Permit {
        self.permit.clone()
    }

    fn borrow(/* finding */ &self) -> &Permit {
        &self.permit
    }

    fn borrow_consumed(/* finding */ self) -> &'static Permit {
        Box::leak(Box::new(self.permit))
    }

    fn raw(/* finding */ self) -> Connection {
        Connection::open_in_memory()
    }
}

// A callback that can keep what the method gives it is handed it; one lent
// it for the call is not, and what a callback returns is its caller's.
pub struct View<'a>(&'a Database);

pub struct Inputs<'a> {
    db: &'a Database,
}

impl<'a> Inputs<'a> {
    pub fn run<T>(/* finding */ &self, work: impl FnOnce(&'a Database) -> T) -> T {
        work(self.db)
    }

    pub fn bound<T, F: FnOnce(&'a Database) -> T>(/* finding */ &self, work: F) -> T {
        work(self.db)
    }

    pub fn pointer(/* finding */ &self, work: fn(&'a Database)) {
        work(self.db)
    }

    pub fn lend<T>(&self, work: impl FnOnce(&Database) -> T) -> T {
        work(self.db)
    }

    pub fn lend_bound<T>(&self, work: impl for<'r> FnOnce(&'r Database) -> T) -> T {
        work(self.db)
    }

    pub fn lend_where<T, F>(&self, work: F) -> T
    where
        F: for<'r> Fn(&'r Database) -> T,
    {
        work(self.db)
    }

    pub fn lend_view(&self, work: impl FnOnce(View<'_>)) {
        work(View(self.db))
    }

    pub fn supplied(&self, work: impl FnOnce() -> &'a Database) {
        work();
    }
}

pub struct Owning {
    db: Database,
}

impl Owning {
    pub fn owned(/* finding */ self, work: &mut dyn FnMut(Database)) {
        work(self.db)
    }
}

// A value the method makes is not the one its owner holds, returned or
// given to a callback; what reads `self`, and a callback passed on, may be.
macro_rules! clock {
    () => {
        Clock::new()
    };
    ($owner:expr) => {
        $owner.clock.clone()
    };
}

pub struct Timer {
    clock: Clock,
    count: u64,
}

impl Timer {
    fn count(&self) -> u64 {
        self.count
    }

    fn with(&self, work: impl FnOnce(Clock)) {
        work(Clock::new())
    }

    pub fn fresh(&self) -> Clock {
        let clock = Clock::new();
        clock
    }

    pub fn held(/* finding */ &self) -> Clock {
        self.clock.clone()
    }

    pub fn through_local(/* finding */ &self) -> Clock {
        let clock = self.clock.clone();
        clock
    }

    pub fn early(/* finding */ &self, late: bool) -> Clock {
        if late {
            return self.clock.clone();
        }
        Clock::new()
    }

    pub fn macro_made(&self) -> Clock {
        clock!()
    }

    pub fn macro_held(/* finding */ &self) -> Clock {
        clock!(self)
    }

    pub fn start(&self, construct: impl FnOnce(Clock, u64)) {
        let clock = Clock::new();
        let count = self.count();
        construct(clock, count)
    }

    pub fn generic<F: FnOnce(Clock)>(&self, construct: F) {
        construct(Clock::new())
    }

    pub fn give(/* finding */ &self, construct: impl FnOnce(Clock)) {
        let clock = self.clock.clone();
        construct(clock)
    }

    pub fn pass_on(/* finding */ &self, construct: impl FnOnce(Clock)) {
        forward(construct)
    }

    pub fn in_closure(/* finding */ &self, construct: impl FnOnce(Clock)) {
        self.with(|clock| construct(clock))
    }

    pub async fn later(/* finding */ &self) -> Clock {
        ready().await;
        self.clock.clone()
    }

    pub async fn later_fresh(&self) -> Clock {
        ready().await;
        Clock::new()
    }

    pub async fn give_later(/* finding */ &self, construct: impl FnOnce(Clock)) {
        ready().await;
        construct(self.clock.clone())
    }

    pub async fn give_later_fresh(&self, construct: impl FnOnce(Clock)) {
        ready().await;
        construct(Clock::new())
    }
}

// What shares an owner's state without being its handle is no handle.
pub struct Glance {
    state: Arc<State>,
}

pub struct Lens {
    state: Arc<State>,
}

impl Lens {
    pub fn glance(/* finding */ &self) -> Glance {
        Glance { state: self.state.clone() }
    }
}

// A trait's method that makes owners returns one when its declaration is a
// listed root.
pub struct Remote {
    clock: Clock,
}

pub trait Connector {
    fn connect(&self) -> Remote;
}

pub trait Dialer {
    fn dial(&self) -> Remote; // finding
}

// Test code answers to none of the rules.
#[cfg(feature = "test-utils")]
impl Timer {
    pub fn test_clock(&self) -> Clock {
        self.clock.clone()
    }
}

#[cfg(feature = "test-utils")]
pub struct Fake {
    pub clock: Clock,
}
