use super::*;
use crate::clock::within;
use futures_lite::future::poll_once;
use std::num::NonZeroUsize;
use std::pin::pin;

fn budget(size: usize) -> FileBudget {
    FileBudget::new(NonZeroUsize::new(size).unwrap())
}

#[tokio::test]
async fn a_lease_waits_until_its_places_come_back() {
    let budget = budget(1);
    let watch = budget.watch();
    let held = budget.lease(1).await.unwrap();
    let waited = watch.lease_waits_from_now(1);
    let waiting = tokio::spawn({
        let budget = budget.clone();
        async move { budget.lease(1).await.map(|_| ()) }
    });
    within(waited).await.unwrap();
    assert!(!waiting.is_finished());
    drop(held);
    within(waiting).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn a_lease_of_more_places_than_the_budget_fails_at_once() {
    let budget = budget(2);
    let held = budget.lease(1).await.unwrap();
    let error = within(budget.lease(3)).await.unwrap().unwrap_err();
    assert_eq!(error, LeaseTooLarge { requested: 3, size: 2 });
    drop(held);
}

#[tokio::test]
async fn a_waiting_lease_of_two_places_goes_before_later_leases_of_one() {
    let budget = budget(2);
    let watch = budget.watch();
    let held = budget.lease(1).await.unwrap();
    let (order, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for count in [2, 1] {
        let waited = watch.lease_waits_from_now(1);
        let (budget, order) = (budget.clone(), order.clone());
        tasks.push(tokio::spawn(async move {
            let lease = budget.lease(count).await.unwrap();
            order.send(count).unwrap();
            drop(lease);
        }));
        // The lease of one fits the free place, and still waits behind the
        // lease of two.
        within(waited).await.unwrap();
    }
    drop(held);
    for task in tasks {
        within(task).await.unwrap().unwrap();
    }
    assert_eq!(arrivals.recv().await, Some(2));
    assert_eq!(arrivals.recv().await, Some(1));
}

#[tokio::test]
async fn a_blocking_lease_waits_in_the_same_line_as_async_ones() {
    let budget = budget(1);
    let watch = budget.watch();
    let held = budget.lease(1).await.unwrap();
    let (order, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let waited = watch.lease_waits_from_now(1);
    let blocking = tokio::task::spawn_blocking({
        let (budget, order) = (budget.clone(), order.clone());
        move || {
            let lease = budget.lease_blocking(1).unwrap();
            order.send("blocking").unwrap();
            drop(lease);
        }
    });
    within(waited).await.unwrap();
    let waited = watch.lease_waits_from_now(1);
    let asynchronous = tokio::spawn({
        let budget = budget.clone();
        async move {
            let lease = budget.lease(1).await.unwrap();
            order.send("async").unwrap();
            drop(lease);
        }
    });
    within(waited).await.unwrap();
    drop(held);
    within(blocking).await.unwrap().unwrap();
    within(asynchronous).await.unwrap().unwrap();
    assert_eq!(arrivals.recv().await, Some("blocking"));
    assert_eq!(arrivals.recv().await, Some("async"));
}

#[tokio::test]
async fn an_open_waits_inside_its_lease_and_never_on_the_budget() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"a").unwrap();
    let budget = budget(2);
    let watch = budget.watch();
    let lease = budget.lease(1).await.unwrap();
    let file = lease.place().await.open(&path).unwrap();
    let mut second = pin!(lease.place());
    assert!(poll_once(&mut second).await.is_none());
    assert_eq!(watch.leased(), 1);
    // The budget has a place left: the wait is inside the lease.
    let other = within(budget.lease(1)).await.unwrap().unwrap();
    assert_eq!(watch.leased(), 2);
    drop(other);
    drop(file);
    within(second).await.unwrap();
}

#[tokio::test]
async fn a_place_comes_back_only_after_its_file_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"a").unwrap();
    let budget = budget(1);
    let lease = budget.lease(1).await.unwrap();
    let file = lease.place().await.open(&path).unwrap();
    let closing = tokio::spawn(async move {
        tokio::task::yield_now().await;
        drop(file);
    });
    let _second = within(lease.place()).await.unwrap();
    crate::files::assert_no_open_files_under(directory.path());
    closing.await.unwrap();
}

#[tokio::test]
async fn a_lease_gives_its_places_back_once_it_and_its_files_are_gone() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"a").unwrap();
    let budget = budget(2);
    let watch = budget.watch();
    let lease = budget.lease(2).await.unwrap();
    let file = lease.place().await.open_async(&path).await.unwrap();
    drop(lease);
    assert_eq!(watch.leased(), 2);
    let waited = watch.lease_waits_from_now(1);
    let blocked = tokio::spawn({
        let budget = budget.clone();
        async move { budget.lease(2).await.map(|_| ()) }
    });
    within(waited).await.unwrap();
    drop(file);
    within(blocked).await.unwrap().unwrap().unwrap();
    assert_eq!(watch.leased(), 0);
}

#[test]
fn the_size_under_a_descriptor_limit_is_an_eighth_of_it() {
    for (limit, size) in [(Some(256), 32), (Some(1024), 128), (Some(7), 1), (Some(0), 1)] {
        assert_eq!(size_for_limit(limit).get(), size, "{limit:?}");
    }
    assert_eq!(size_for_limit(None).get(), 1280);
}

#[tokio::test]
async fn the_watch_keeps_the_most_files_open_at_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"a").unwrap();
    let budget = budget(4);
    let watch = budget.watch();
    let lease = budget.lease(3).await.unwrap();
    let first = lease.place().await.open(&path).unwrap();
    let second = lease.place().await.open(&path).unwrap();
    assert_eq!((watch.holding(), watch.most_holding()), (2, 2));
    drop(first);
    assert_eq!((watch.holding(), watch.most_holding()), (1, 2));
    drop(second);
    assert_eq!((watch.holding(), watch.most_holding()), (0, 2));
    assert_eq!((watch.leased(), watch.most_leased()), (3, 3));
    drop(lease);
    assert_eq!((watch.leased(), watch.most_leased()), (0, 3));
}
