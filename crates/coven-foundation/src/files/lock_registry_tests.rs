use super::*;
use crate::clock::HANG_GUARD;
use crate::files::StoreDirAccess;
use crate::{files::StoreDirectories, id_source::UuidIds};
use std::{
    future::{poll_fn, Future},
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cancelling_filesystem_lock_acquisition_keeps_the_runtime_available_for_reopen() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared());
    let id = StoreId(uuid::Uuid::from_u128(5));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    let layout_lock = crate::files::lock::lock_layout(&root.path().join("stores")).unwrap();
    let (polled, polling) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move {
            let mut opening = Box::pin(store.lock_for_open());
            let first = poll_fn(|cx| Poll::Ready(opening.as_mut().poll(cx))).await;
            let lock = match first {
                Poll::Ready(result) => {
                    polled.send(false).unwrap();
                    result.unwrap()
                }
                Poll::Pending => {
                    drop(opening);
                    let mut reopened = Box::pin(store.lock_for_open());
                    let next = poll_fn(|cx| Poll::Ready(reopened.as_mut().poll(cx))).await;
                    polled.send(next.is_pending()).unwrap();
                    match next {
                        Poll::Ready(result) => result.unwrap(),
                        Poll::Pending => reopened.await.unwrap(),
                    }
                }
            };
            drop(lock);
        });
    });
    let pending = polling.recv_timeout(HANG_GUARD);
    drop(layout_lock);
    worker.join().unwrap();
    assert_eq!(pending, Ok(true), "filesystem locking blocked the runtime");
}

#[tokio::test]
async fn closing_waiters_share_canonical_identity_and_only_one_becomes_live() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared());
    let id = StoreId(uuid::Uuid::from_u128(1));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    let lock = store.lock_for_open().await.unwrap();
    let owner = Arc::new(lock.closing_guard());
    let clone = owner.clone();
    drop(owner);
    assert!(
        matches!(store.lock_for_open().await, Err(StoreLockError::AlreadyOpen(found)) if found == id)
    );
    drop(clone);

    let alias = StoreDirectories::new(root.path().join("."), crate::task::HeardBugs::shared())
        .store_dir(&id);
    let mut first = Box::pin(alias.lock_for_open());
    let mut second = Box::pin(store.lock_for_open());
    let mut cancelled = Box::pin(store.lock_for_open());
    let first_wakes = Arc::new(WakeCount::default());
    let second_wakes = Arc::new(WakeCount::default());
    let first_waker = std::task::Waker::from(first_wakes.clone());
    let second_waker = std::task::Waker::from(second_wakes.clone());
    let writer = crate::files::writer_lock(store.database_path().parent().unwrap(), id);
    let mut waits = crate::files::when_reached(&writer);
    assert!(first.as_mut().poll(&mut std::task::Context::from_waker(&first_waker)).is_pending());
    waits.try_recv().expect("the alias waits for the closing writer");
    let mut waits = crate::files::when_reached(&writer);
    assert!(second.as_mut().poll(&mut std::task::Context::from_waker(&second_waker)).is_pending());
    waits.try_recv().expect("the second open waits for the closing writer");
    let mut waits = crate::files::when_reached(&writer);
    poll_fn(|cx| {
        assert!(cancelled.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    waits.try_recv().expect("the cancelled open waited for the closing writer");
    drop(cancelled);
    // The same id under another layout is unrelated to this closing store.
    let elsewhere = tempfile::tempdir().unwrap();
    let other = StoreDirectories::new(elsewhere.path().into(), crate::task::HeardBugs::shared())
        .create_store_dir(id, &UuidIds)
        .unwrap();
    drop(other.lock_for_open().await.unwrap());
    assert_eq!(first_wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(second_wakes.0.load(Ordering::SeqCst), 0);
    drop(lock);
    assert!(first_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(second_wakes.0.load(Ordering::SeqCst) > 0);
    let next = first.await.unwrap();
    assert!(matches!(second.await, Err(StoreLockError::AlreadyOpen(found)) if found == id));
    drop(next);
    drop(store.lock_for_open().await.unwrap());
}

#[tokio::test]
async fn a_released_guard_cannot_mark_a_replacement_as_closing() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared());
    let id = StoreId(uuid::Uuid::from_u128(2));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    let lock = store.lock_for_open().await.unwrap();
    let old_owner = lock.closing_guard();
    drop(lock);
    let replacement = store.lock_for_open().await.unwrap();
    drop(old_owner);
    assert!(
        matches!(store.lock_for_open().await, Err(StoreLockError::AlreadyOpen(found)) if found == id)
    );
    drop(replacement);
}

/// Dropping a bootstrap releases its lock at once, and the directories it
/// gave out hold none of it, even while an owner keeps one (api.md E10). A
/// second bootstrap of the store waits only for the dropped one's remaining
/// work: the writer its directory opened, now closing. The reopen tells that
/// wait at the writer's lock, and finishes once the writer drops, with the
/// directory still held.
#[tokio::test]
async fn bootstrap_reopen_waits_for_the_dropped_bootstraps_writer_and_not_its_directory() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared());
    let id = StoreId(uuid::Uuid::from_u128(3));
    let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    let directory = pending.directory();
    let writer = directory.lock_for_open().await.unwrap();
    let parent = directory.database_path().parent().unwrap().to_owned();
    let mut waits = crate::files::when_reached(&crate::files::writer_lock(&parent, id));
    drop(pending);
    let mut reopen = Box::pin(layout.begin_bootstrap(id, &UuidIds));
    tokio::select! {
        biased;
        waited = crate::clock::within(&mut waits) => {
            waited.expect("the reopen waits for the closing writer").unwrap();
        }
        _ = reopen.as_mut() => panic!("the reopen finished without waiting"),
    }
    drop(writer);
    let resumed =
        crate::clock::within(reopen).await.expect("the reopen ends once the writer drops").unwrap();
    drop(resumed.directory().lock_for_open().await.unwrap());
    resumed.cancel().unwrap();
    drop(directory);
}

#[cfg(unix)]
#[tokio::test]
async fn a_layout_symlink_observes_the_same_closing_writer() {
    let root = tempfile::tempdir().unwrap();
    let alias_root = tempfile::tempdir().unwrap();
    let alias = alias_root.path().join("alias");
    std::os::unix::fs::symlink(root.path(), &alias).unwrap();
    let id = StoreId(uuid::Uuid::from_u128(4));
    let store = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared())
        .create_store_dir(id, &UuidIds)
        .unwrap();
    let lock = store.lock_for_open().await.unwrap();
    drop(lock.closing_guard());
    let aliased_store =
        StoreDirectories::new(alias, crate::task::HeardBugs::shared()).store_dir(&id);
    let mut reopen = Box::pin(aliased_store.lock_for_open());
    let mut waits = crate::files::when_reached(&crate::files::writer_lock(
        store.database_path().parent().unwrap(),
        id,
    ));
    poll_fn(|cx| {
        assert!(reopen.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    waits.try_recv().expect("the aliased open waits for the closing writer");
    drop(lock);
    drop(reopen.await.unwrap());
}

/// One reader beginning to close leaves the store's other readers live, a
/// writer opens beside it without waiting, and waiting for the store's
/// closing instances ends once that reader's lock is released.
#[tokio::test]
async fn a_closing_reader_is_waited_for_only_by_closed_and_only_until_released() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into(), crate::task::HeardBugs::shared());
    let id = StoreId(uuid::Uuid::from_u128(3));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    let closing = store.lock_read_only().unwrap();
    let other = store.lock_read_only().unwrap();
    closing.closing_guard().begin_close();
    let writer = tokio::time::timeout(HANG_GUARD, store.lock_for_open())
        .await
        .expect("a writer doesn't wait on a closing reader")
        .unwrap();
    let mut waiting = Box::pin(store.closed());
    assert!(poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await.is_pending());
    drop(closing);
    tokio::time::timeout(HANG_GUARD, waiting).await.expect("released").unwrap();
    drop(writer);
    assert!(matches!(store.lock_for_deletion(), Err(StoreLockError::AlreadyOpen(_))));
    drop(other);
    assert!(store.lock_for_deletion().unwrap().is_some());
}
