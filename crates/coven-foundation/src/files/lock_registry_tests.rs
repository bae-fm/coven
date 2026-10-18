use super::*;
use crate::files::{StoreDirAccess, StoreDirectoriesAccess};
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
    let layout = StoreDirectories::new(root.path().into());
    let id = StoreId(uuid::Uuid::from_u128(5));
    let store = layout.create_store_dir(id, &UuidIds).unwrap();
    let layout_lock = crate::files::lock::lock_layout(&root.path().join("stores")).unwrap();
    let (polled, polling) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async move {
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
    let pending = polling.recv_timeout(std::time::Duration::from_secs(5));
    drop(layout_lock);
    worker.join().unwrap();
    assert_eq!(pending, Ok(true), "filesystem locking blocked the runtime");
}

#[tokio::test]
async fn closing_waiters_share_canonical_identity_and_only_one_becomes_live() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreDirectories::new(root.path().into());
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

    let alias = StoreDirectories::new(root.path().join(".")).store_dir(&id);
    let mut first = Box::pin(alias.lock_for_open());
    let mut second = Box::pin(store.lock_for_open());
    let mut cancelled = Box::pin(store.lock_for_open());
    let first_wakes = Arc::new(WakeCount::default());
    let second_wakes = Arc::new(WakeCount::default());
    let first_waker = std::task::Waker::from(first_wakes.clone());
    let second_waker = std::task::Waker::from(second_wakes.clone());
    assert!(first
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&first_waker))
        .is_pending());
    assert!(second
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&second_waker))
        .is_pending());
    poll_fn(|cx| {
        assert!(cancelled.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(cancelled);
    // The same id under another layout is unrelated to this closing store.
    let elsewhere = tempfile::tempdir().unwrap();
    let other = StoreDirectories::new(elsewhere.path().into())
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
    let layout = StoreDirectories::new(root.path().into());
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

#[tokio::test]
async fn bootstrap_reopen_waits_for_both_the_lease_and_the_writer_in_either_drop_order() {
    for writer_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreDirectories::new(root.path().into());
        let id = StoreId(uuid::Uuid::from_u128(3));
        let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
        let directory = pending.directory();
        let writer = directory.lock_for_open().await.unwrap();
        drop(pending);
        let mut reopen = Box::pin(layout.begin_bootstrap(id, &UuidIds));
        poll_fn(|cx| {
            assert!(reopen.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if writer_first {
            drop(writer);
            poll_fn(|cx| {
                assert!(reopen.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(directory);
        } else {
            drop(directory);
            poll_fn(|cx| {
                assert!(reopen.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(writer);
        }
        let resumed = reopen.await.unwrap();
        drop(resumed.directory().lock_for_open().await.unwrap());
        resumed.cancel().unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_layout_symlink_observes_the_same_closing_writer() {
    let root = tempfile::tempdir().unwrap();
    let alias_root = tempfile::tempdir().unwrap();
    let alias = alias_root.path().join("alias");
    std::os::unix::fs::symlink(root.path(), &alias).unwrap();
    let id = StoreId(uuid::Uuid::from_u128(4));
    let store = StoreDirectories::new(root.path().into())
        .create_store_dir(id, &UuidIds)
        .unwrap();
    let lock = store.lock_for_open().await.unwrap();
    drop(lock.closing_guard());
    let aliased_store = StoreDirectories::new(alias).store_dir(&id);
    let mut reopen = Box::pin(aliased_store.lock_for_open());
    poll_fn(|cx| {
        assert!(reopen.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(lock);
    drop(reopen.await.unwrap());
}
