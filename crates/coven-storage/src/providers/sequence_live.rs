//! Shared live sequence checks. Printed results are the test's evidence protocol.

use crate::{ObjectPath, Storage, StorageError, StorageFailure, StoredObject};
use coven_foundation::{clock::Clock, id_source::StoreId};
use futures_util::future::join_all;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::Barrier;

pub(crate) const STAGES: [(usize, u64); 2] = [(2, 200), (8, 50)];

pub(crate) struct Reply {
    pub(crate) writer: usize,
    pub(crate) start: Duration,
    pub(crate) end: Duration,
    pub(crate) result: Result<(), StorageError>,
}

/// All intervals must share an instant; a barrier alone does not establish it.
pub(crate) fn judge(replies: &[Reply], payloads: &[Vec<u8>], stored: &[u8]) -> bool {
    let latest_start = replies.iter().map(|reply| reply.start).max();
    let earliest_end = replies.iter().map(|reply| reply.end).min();
    let winners: Vec<_> = replies
        .iter()
        .filter(|reply| reply.result.is_ok())
        .collect();
    latest_start
        .zip(earliest_end)
        .is_some_and(|(start, end)| start < end)
        && winners.len() == 1
        && stored == payloads[winners[0].writer]
        && replies.iter().all(|reply| match &reply.result {
            Ok(()) => true,
            Err(error) => {
                !matches!(error, StorageError::Cleanup { .. })
                    && matches!(
                        error.failure(),
                        StorageFailure::AlreadyExists | StorageFailure::Conflict
                    )
            }
        })
}

fn require(ok: bool, reason: &'static str) -> Result<(), StorageError> {
    if ok {
        Ok(())
    } else {
        Err(StorageFailure::Protocol.with_source(reason))
    }
}

fn expect_failure(error: StorageError, expected: StorageFailure) -> Result<(), StorageError> {
    if !matches!(error, StorageError::Cleanup { .. }) && error.failure() == expected {
        Ok(())
    } else {
        Err(error)
    }
}

/// Await the real mutation, then withhold its successful reply from the caller.
/// Provider failures remain failures; no request is cancelled or left in flight.
async fn lose_reply<T>(
    request: impl Future<Output = Result<T, StorageError>>,
) -> Result<(), StorageError> {
    request.await?;
    Err(StorageFailure::Network.into())
}

/// A lost slot reply and a lost floor reply settle from exact reads. In each
/// case a stale retry must preserve the confirmed bytes and revision.
pub(crate) async fn lost_replies(
    writer: &dyn Storage,
    reader: &dyn Storage,
    store: StoreId,
) -> Result<(), StorageError> {
    let slot = ObjectPath::slot(store, std::num::NonZeroU64::MIN);
    let bytes = b"slot whose successful create reply was withheld";
    let reply = lose_reply(writer.create(&slot, bytes)).await.unwrap_err();
    expect_failure(reply, StorageFailure::Network)?;
    require(
        reader.read(&slot).await? == bytes,
        "lost slot reply did not settle to the recorded bytes",
    )?;
    let retry = writer.create(&slot, bytes).await;
    match retry {
        Ok(_) => {}
        Err(error)
            if !matches!(error, StorageError::Cleanup { .. })
                && error.failure() == StorageFailure::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    require(
        reader.read(&slot).await? == bytes,
        "slot retry changed bytes",
    )?;
    let different = reader.create(&slot, b"another claim").await.unwrap_err();
    expect_failure(different, StorageFailure::AlreadyExists)?;
    require(
        writer.read(&slot).await? == bytes,
        "refused slot retry changed bytes",
    )?;

    let floor = ObjectPath::floor(store);
    writer.create(&floor, b"first=1").await?;
    let previous = reader.read_object(&floor, None).await?.metadata;
    let reply = lose_reply(writer.replace(&previous, b"first=2"))
        .await
        .unwrap_err();
    expect_failure(reply, StorageFailure::Network)?;
    let confirmed = reader.read_object(&floor, None).await?;
    require(
        confirmed.bytes == b"first=2" && confirmed.metadata.revision != previous.revision,
        "lost floor reply did not settle to the new bytes and revision",
    )?;
    for bytes in [b"first=2", b"first=9"] {
        let retry = reader.replace(&previous, bytes).await.unwrap_err();
        expect_failure(retry, StorageFailure::Conflict)?;
        let retained = writer.read_object(&floor, None).await?;
        require(
            retained.bytes == confirmed.bytes && retained.metadata == confirmed.metadata,
            "stale floor retry changed the confirmed floor",
        )?;
    }
    println!("sequence lost replies: slot equal/different retry=PASS floor read/stale retry=PASS");
    Ok(())
}

/// Distinct connections share the store; callers alternate accounts so every
/// width includes both. The first slot and floor were seeded by lost_replies.
pub(crate) async fn slots(
    clock: &dyn Clock,
    writers: &[Arc<dyn Storage>],
    store: StoreId,
) -> Result<(), StorageError> {
    let mut slot = 1;
    for (width, trials) in STAGES {
        for trial in 1..=trials {
            slot += 1;
            run(
                clock,
                &writers[..width],
                &ObjectPath::slot(store, slot.try_into().unwrap()),
                None,
                trial,
            )
            .await?;
        }
        println!("sequence SUMMARY create writers={width} trials={trials} overlapping={trials} winners={trials} violations=0");
    }
    Ok(())
}

pub(crate) async fn floor(
    clock: &dyn Clock,
    writers: &[Arc<dyn Storage>],
    store: StoreId,
) -> Result<(), StorageError> {
    let path = ObjectPath::floor(store);
    for (width, trials) in STAGES {
        for trial in 1..=trials {
            // Every replacer observes the same current revision before release.
            let previous = writers[0].read_object(&path, None).await?.metadata;
            for writer in &writers[1..width] {
                require(
                    writer.read_object(&path, None).await?.metadata == previous,
                    "replacers did not observe the same floor revision",
                )?;
            }
            run(clock, &writers[..width], &path, Some(&previous), trial).await?;
        }
        println!("sequence SUMMARY floor writers={width} trials={trials} overlapping={trials} winners={trials} violations=0");
    }
    Ok(())
}

async fn run(
    clock: &dyn Clock,
    writers: &[Arc<dyn Storage>],
    path: &ObjectPath,
    previous: Option<&StoredObject>,
    trial: u64,
) -> Result<(), StorageError> {
    let width = writers.len();
    let kind = if previous.is_some() {
        "floor"
    } else {
        "create"
    };
    let payloads: Vec<_> = (0..width)
        .map(|writer| {
            let mut bytes = format!("{kind} width={width} trial={trial} writer={writer} ")
                .repeat(4096)
                .into_bytes();
            bytes.truncate(64 * 1024);
            bytes
        })
        .collect();
    let barrier = Barrier::new(width);
    let origin = clock.elapsed();
    // join_all awaits every request even when another fails, before cleanup.
    let replies = join_all(writers.iter().enumerate().map(|(writer, storage)| {
        let barrier = &barrier;
        let bytes = &payloads[writer];
        async move {
            barrier.wait().await;
            let start = clock.elapsed() - origin;
            let result = match previous {
                Some(previous) => storage.replace(previous, bytes).await,
                None => storage.create(path, bytes).await.map(drop),
            };
            Reply {
                writer,
                start,
                end: clock.elapsed() - origin,
                result,
            }
        }
    }))
    .await;
    let stored = writers[0].read_object(path, None).await?;
    let passed = judge(&replies, &payloads, &stored.bytes)
        && previous.is_none_or(|previous| previous.revision != stored.metadata.revision)
        && replies.iter().all(|reply| match &reply.result {
            Ok(()) => true,
            Err(error) => previous.is_none() || error.failure() == StorageFailure::Conflict,
        });
    if trial == 1 || !passed {
        for reply in &replies {
            println!("sequence {kind} writers={width} trial={trial} writer={} start_us={} end_us={} outcome={:?}",
                reply.writer, reply.start.as_micros(), reply.end.as_micros(), reply.result);
        }
    }
    require(
        passed,
        "sequence race failed reply, byte, revision or overlap checks",
    )?;
    require(
        writers[width - 1].read(path).await? == stored.bytes,
        "sequence race readers disagree on the winning bytes",
    )?;
    if trial.is_multiple_of(25) {
        println!("sequence {kind} writers={width} completed={trial}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "sequence_live_tests.rs"]
mod tests;
