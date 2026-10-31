use super::OpenStep;
use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Migration, RowIdentity, SqlFunctions, SyncedTable};
use coven_foundation::clock::FixedClock;
use coven_foundation::progress::{forward, Progress};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime};

/// What the host was called with, which the open's own steps can wait on.
#[derive(Clone, Default)]
struct Seen(Arc<(Mutex<Vec<Progress<OpenStep>>>, Condvar)>);

impl Seen {
    fn push(&self, progress: Progress<OpenStep>) {
        self.0 .0.lock().unwrap().push(progress);
        self.0 .1.notify_all();
    }

    /// Wait until the host has been called with `progress`. A wait that
    /// runs out returns, and the test fails on what the host saw.
    fn wait_for(&self, progress: Progress<OpenStep>) {
        let (seen, changed) = &*self.0;
        let (_seen, _timed_out) = changed
            .wait_timeout_while(seen.lock().unwrap(), Duration::from_secs(10), |seen| {
                !seen.contains(&progress)
            })
            .unwrap();
    }

    fn all(&self) -> Vec<Progress<OpenStep>> {
        self.0 .0.lock().unwrap().clone()
    }
}

fn at(step: OpenStep, done: u64, total: u64) -> Progress<OpenStep> {
    Progress { step, done, total }
}

/// Steps arrive in the order the open runs them, each step's total stays
/// put, and within a step `done` never goes backwards or passes its total.
fn assert_honest(seen: &[Progress<OpenStep>]) {
    let order = [OpenStep::Rebuilding, OpenStep::Migrating];
    let position = |step| order.iter().position(|s| *s == step).unwrap();
    for progress in seen {
        assert!(progress.done <= progress.total, "{seen:?}");
    }
    for pair in seen.windows(2) {
        let [before, after] = pair else { unreachable!() };
        assert!(position(before.step) <= position(after.step), "{seen:?}");
        if before.step == after.step {
            assert_eq!(before.total, after.total, "{seen:?}");
            assert!(before.done <= after.done, "{seen:?}");
        }
    }
}

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

const WORDS: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
     CREATE TABLE words(word TEXT NOT NULL);
     CREATE INDEX words_ordered ON words(word COLLATE ordered)";

/// `ordered` at version 1 sorts text backwards; version 2 sorts it forwards,
/// first waiting until the host has seen the rebuild begin.
fn ordered(version: u32, seen: &Seen) -> SqlFunctions {
    let seen = seen.clone();
    SqlFunctions::default().collation("ordered", version, move |left: &str, right: &str| {
        if version == 1 {
            return right.cmp(left);
        }
        seen.wait_for(at(OpenStep::Rebuilding, 0, 1));
        left.cmp(right)
    })
}

/// Ana's app 2.0 sorts `ordered` forwards and adds two migrations. Opening
/// her store rebuilds the index in that order, then runs the migrations,
/// and the host hears each step as it runs: the rebuild's beginning, a
/// migration finished a second after the step began, and the last one.
/// Each of those waits until the host has seen the report before, so none
/// is replaced by a newer one before the host looks.
#[tokio::test]
async fn opening_reports_its_steps_in_order() {
    let store = TestStore::new();
    let seen = Seen::default();
    let first = vec![Migration::addition(1, "words", WORDS)];
    let database = store
        .builder(notes(), first.clone())
        .sql_functions(ordered(1, &seen))
        .open()
        .await
        .unwrap();
    sql(&database, "INSERT INTO words VALUES ('apples'),('bananas'),('cherries')").await.unwrap();
    database.close().await.unwrap();

    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let mut migrations = first;
    let later = clock.clone();
    migrations.push(Migration::addition_with(2, "tags", move |sql| {
        sql.execute_batch("CREATE TABLE tags(tag TEXT NOT NULL)")?;
        later.set(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
        Ok(())
    }));
    let waiting = seen.clone();
    migrations.push(Migration::addition_with(3, "colors", move |sql| {
        waiting.wait_for(at(OpenStep::Migrating, 1, 2));
        sql.execute_batch("CREATE TABLE colors(color TEXT NOT NULL)")?;
        Ok(())
    }));
    let builder = store.builder(notes(), migrations).sql_functions(ordered(2, &seen)).clock(clock);
    let progress = builder.subscribe_progress();
    let host = seen.clone();
    let database = forward(progress, builder.open(), |progress| host.push(progress)).await.unwrap();
    database.close().await.unwrap();

    let seen = seen.all();
    assert_honest(&seen);
    for expected in [at(OpenStep::Rebuilding, 0, 1), at(OpenStep::Migrating, 1, 2)] {
        assert!(seen.contains(&expected), "{expected:?} in {seen:?}");
    }
    assert_eq!(seen.last(), Some(&at(OpenStep::Migrating, 2, 2)));
}

/// A new store's first open runs its migrations, and reports them; the
/// next open has nothing to rebuild or migrate, and reports no step.
#[tokio::test]
async fn an_open_with_nothing_to_do_reports_nothing() {
    let store = TestStore::new();
    let migrations = vec![Migration::addition(1, "words", WORDS)];
    let open = || async {
        let seen = Seen::default();
        let builder = store.builder(notes(), migrations.clone()).sql_functions(ordered(1, &seen));
        let progress = builder.subscribe_progress();
        let host = seen.clone();
        let database =
            forward(progress, builder.open(), |progress| host.push(progress)).await.unwrap();
        database.close().await.unwrap();
        seen.all()
    };
    let created = open().await;
    assert_honest(&created);
    assert_eq!(created.last(), Some(&at(OpenStep::Migrating, 1, 1)));
    assert!(open().await.is_empty());
}
