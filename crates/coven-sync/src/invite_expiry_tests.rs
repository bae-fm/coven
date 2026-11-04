use super::expire;
use crate::sequence_sync::sequence_invites::tests::requests;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::{operation_kind, DbError};
use coven_foundation::clock::Clock;
use std::sync::Arc;

/// A wall clock a person sets by hand, as on a phone: its elapsed time and
/// its sleeps never move, and its wall waits are the shared clock's.
struct HandSetClock(Arc<coven_foundation::clock::FixedClock>);

impl Clock for HandSetClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
    fn elapsed(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
    fn sleep(
        &self,
        _duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::pending())
    }
    fn sleep_until(
        &self,
        at: std::time::SystemTime,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), coven_foundation::clock::WallTimerError>>
                + Send,
        >,
    > {
        self.0.sleep_until(at)
    }
}

/// Ana sets her phone's clock by hand while an invite is open, with no time
/// passing. The handle wakes when the wall clock reaches the invite's
/// expiry, however it gets there: set back a day, then forward to a second
/// before the expiry, the invite stays open; set to its expiry, the handle
/// ends it at once (§13.2).
#[tokio::test]
async fn the_expiry_wake_follows_a_clock_set_by_hand() {
    use std::time::Duration;
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let expiry = InviteExpiry::start(
        ana.sync.database.clone(),
        Arc::new(HandSetClock(group.clock.clone())),
        ana.sync.waits.clone(),
        ana.sync.state.clone(),
    );
    let mut open = ana.sync.database.subscribe(|sql| Ok(Invite::list(sql)?.len()));
    assert_eq!(open.next().await.unwrap().unwrap(), 1);
    group.clock.set(group.clock.now() - Duration::from_secs(86_400));
    group.clock.set(invite.expires_at - Duration::from_secs(1));
    let early = tokio::time::timeout(Duration::from_millis(200), open.next()).await;
    assert!(early.is_err(), "the invite ended before its expiry: {early:?}");
    group.clock.set(invite.expires_at);
    let ended = tokio::time::timeout(Duration::from_secs(10), open.next()).await;
    assert_eq!(ended.expect("the handle woke at the expiry").unwrap().unwrap(), 0);
    expiry.close().await;
    ana.sync.database.close().await.unwrap();
}

/// A clock that can't wait for an open invite's expiry fails the handle
/// with its cause, rather than leave the invite open past its lifetime
/// (§13.2).
#[tokio::test]
async fn a_clock_that_cant_wait_for_an_expiry_fails_the_handle() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    invite(&ana).await;
    let now = group.clock.now();
    let expiry = InviteExpiry::start(
        ana.sync.database.clone(),
        Arc::new(coven_foundation::clock::ClosureClock(move || now)),
        ana.sync.waits.clone(),
        ana.sync.state.clone(),
    );
    let mut open = ana.sync.database.subscribe(|sql| Ok(Invite::list(sql)?.len()));
    let failure = loop {
        match open.next().await.expect("the live query reports the failure") {
            Ok(_) => continue,
            Err(error) => break error,
        }
    };
    assert!(
        matches!(failure, coven_database::CovenError::Database(DbError::WallTimer(_))),
        "{failure:?}"
    );
    let read = ana.sync.database.read(Invite::list).await;
    assert!(
        matches!(read, Err(coven_database::CovenError::Database(DbError::WallTimer(_)))),
        "{read:?}"
    );
    expiry.close().await;
    ana.sync.database.close().await.unwrap();
}

/// Ana's invite expires while a pass is looking for its request. The pass
/// read the invite before the expiry ended it, and finds no request: that
/// finding belongs to a step the invite has left, so it records no wait over
/// the expiry, and the next pass finishes the ended invite (§13.2, §11.4).
#[tokio::test]
async fn a_pass_under_way_records_nothing_over_an_expiry() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let operation =
        ana.database().first_operation(operation_kind::INVITE).await.unwrap().unwrap().id;
    let (observed, read) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    ana.storage.hold_next_read(request_path(&invite), observed, held).await;
    let expire = async {
        read.await.unwrap();
        group.clock.set(invite.expires_at);
        let (waits, state) = (&ana.sync.waits, &ana.sync.state);
        expire(&ana.sync.database, group.clock.now(), waits, state).await.unwrap();
        resume.send(()).unwrap();
    };
    let (passed, ()) = tokio::join!(ana.sync.pass(), expire);
    passed.unwrap();
    assert!(listed(&ana).await.is_empty());
    assert!(ana.database().operation_pending::<PendingRecord>(operation).await.unwrap().is_none());
    assert!(ana
        .database()
        .retry_wait(coven_database::RetrySubject::Operation(operation))
        .await
        .unwrap()
        .is_none());
    ana.sync.pass().await.unwrap();
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

/// Ana declines Carol's request a moment before the invite's expiry, and
/// no pass has run since. The decline ended the invite, so the expiry at
/// its time finds it no longer open and writes nothing over the decline's
/// work (§13.2).
#[tokio::test]
async fn an_expiry_after_a_decision_writes_nothing() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let invite = invite(&ana).await;
    let carol = join(&group, invite.shown_code(), "Carol's phone").await;
    ana.sync.pass().await.unwrap();
    let request = requests(&ana).await.remove(0);
    let (now, waits, state) = (group.clock.now(), ana.sync.waits.clone(), ana.sync.state.clone());
    ana.sync
        .database
        .write(move |sql| {
            crate::decline_join_request(&sql, now, waits, state, &request)
                .map_err(|error| DbError::SyncStream(Box::new(error)))
        })
        .await
        .unwrap();
    let declined = ana.database().first_operation(operation_kind::INVITE).await.unwrap().unwrap();
    group.clock.set(invite.expires_at);
    let (waits, state) = (&ana.sync.waits, &ana.sync.state);
    let next = expire(&ana.sync.database, group.clock.now(), waits, state).await.unwrap();
    assert_eq!(next, None);
    let after = ana.database().first_operation(operation_kind::INVITE).await.unwrap().unwrap();
    assert_eq!((after.last_step, after.data), (declined.last_step, declined.data));
    for device in [&ana, &carol.device] {
        device.sync.database.close().await.unwrap();
    }
}
