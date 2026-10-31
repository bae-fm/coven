use super::*;
use crate::clock::Clock;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// A clock whose elapsed time moves only when the test moves it.
struct SteppedClock(Mutex<Duration>);

impl SteppedClock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

impl Clock for SteppedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + self.elapsed()
    }
    fn elapsed(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Copying,
    Checking,
}

fn at(step: Step, done: u64, total: u64) -> Option<Progress<Step>> {
    Some(Progress { step, done, total })
}

/// A step's beginning and its last unit are reported at once; units
/// between them wait until a quarter second has passed since the report
/// before, so a step of many quick units reports a few times a second.
#[test]
fn a_report_waits_its_interval_except_at_a_steps_edges() {
    let clock = Arc::new(SteppedClock(Mutex::new(Duration::from_secs(5))));
    let (sender, receiver) = watch::channel(None);
    let mut report = ProgressReport::new(sender, clock.clone());

    report.begin(Step::Copying, 4);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 0, 4));
    report.done(1);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 0, 4));
    clock.advance(REPORT_INTERVAL - Duration::from_millis(1));
    report.done(2);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 0, 4));
    clock.advance(Duration::from_millis(1));
    report.done(3);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 3, 4));
    report.done(4);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 4, 4));

    report.begin(Step::Checking, 2);
    assert_eq!(*receiver.borrow(), at(Step::Checking, 0, 2));
}

/// Slots that land while Ana's laptop reads raise the step's total. The
/// raised total waits its interval like any report; `done` never passes it,
/// and reaching it is reported at once.
#[test]
fn a_grown_total_waits_its_interval_and_reaching_it_is_reported() {
    let clock = Arc::new(SteppedClock(Mutex::new(Duration::ZERO)));
    let (sender, receiver) = watch::channel(None);
    let mut report = ProgressReport::new(sender, clock.clone());

    report.begin(Step::Copying, 2);
    report.advance();
    report.advance();
    assert_eq!(*receiver.borrow(), at(Step::Copying, 2, 2));
    report.grow(4);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 2, 2));
    clock.advance(REPORT_INTERVAL);
    report.advance();
    assert_eq!(*receiver.borrow(), at(Step::Copying, 3, 4));
    report.advance();
    assert_eq!(*receiver.borrow(), at(Step::Copying, 4, 4));
}

/// An operation that starts over, as a restore does when the floor moves
/// while it loads, begins its first step again.
#[test]
fn a_restarted_operation_begins_its_steps_again() {
    let clock = Arc::new(SteppedClock(Mutex::new(Duration::ZERO)));
    let (sender, receiver) = watch::channel(None);
    let mut report = ProgressReport::new(sender, clock);

    report.begin(Step::Copying, 3);
    report.advance();
    report.restart();
    report.begin(Step::Copying, 5);
    assert_eq!(*receiver.borrow(), at(Step::Copying, 0, 5));
}

/// Forwarding calls the host with each value it sees, in order, and with
/// the last one reported just before the work ended; it never repeats one.
#[tokio::test]
async fn forwarding_delivers_the_newest_value_and_the_last_once() {
    let clock = Arc::new(SteppedClock(Mutex::new(Duration::ZERO)));
    let (sender, receiver) = watch::channel(None);
    let seen = Mutex::new(Vec::new());
    let (go, mut gate) = watch::channel(0);
    let work = async move {
        let mut report = ProgressReport::new(sender, clock);
        report.begin(Step::Copying, 2);
        gate.wait_for(|step| *step >= 1).await.unwrap();
        report.done(2);
        report.begin(Step::Checking, 1);
        report.done(1);
        "done"
    };
    let delivered = forward(receiver, work, |progress| {
        seen.lock().unwrap().push(progress);
        go.send_replace(1);
    })
    .await;
    assert_eq!(delivered, "done");
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.first().copied(), at(Step::Copying, 0, 2));
    assert_eq!(seen.last().copied(), at(Step::Checking, 1, 1));
    for pair in seen.windows(2) {
        assert_ne!(pair[0], pair[1]);
    }
}

/// A watch that already holds an earlier operation's last report, as a
/// store's loading watch does after a join's approval check loaded it,
/// delivers nothing for work that reports nothing.
#[tokio::test]
async fn forwarding_never_delivers_an_earlier_operations_report() {
    let (sender, _) = watch::channel(at(Step::Checking, 1, 1));
    let seen = Mutex::new(Vec::new());
    forward(sender.subscribe(), async {}, |progress| seen.lock().unwrap().push(progress)).await;
    assert!(seen.into_inner().unwrap().is_empty());
}
