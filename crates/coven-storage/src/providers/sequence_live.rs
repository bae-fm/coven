//! Shared live sequence checks. Printed results are the test's evidence protocol.

use crate::{ObjectPath, Storage, StorageError, StorageFailure};
use coven_foundation::{clock::Clock, id_source::StoreId};
use futures_util::future::join_all;
use std::{future::Future, ops::AsyncFnMut, sync::Arc, time::Duration};
use tokio::sync::Barrier;

pub(crate) const STAGES: [(usize, u64); 2] = [(2, 200), (8, 50)];

/// Whether a floor round may refuse every writer. OneDrive can refuse both
/// upload sessions of one race; such a round must leave the floor's bytes and
/// revision unchanged, and the caller rereads the floor and retries.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AllRefused {
    /// Every counted round has exactly one winner.
    Fails,
    /// A round may refuse every writer if the stored floor is unchanged.
    Allowed,
}

/// What one judged round established.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Round {
    /// Exactly one writer succeeded and readers hold its bytes.
    Won,
    /// Every writer was refused and the floor is unchanged.
    AllRefused,
}

/// Rounds a stage discarded or retried besides its counted winners.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    /// Throttled rounds, never judged as contention.
    pub(crate) throttled: u64,
    /// Accepted rounds in which every writer was refused.
    pub(crate) refused: u64,
}

pub(crate) struct Reply {
    pub(crate) writer: usize,
    pub(crate) start: Duration,
    pub(crate) end: Duration,
    pub(crate) result: Result<(), StorageError>,
}

/// Reject known violations before a throttled read can postpone inspection.
pub(crate) fn replies_valid(replies: &[Reply]) -> Result<(), StorageError> {
    require(
        replies.iter().filter(|r| r.result.is_ok()).count() <= 1,
        "multiple race writers succeeded",
    )?;
    require(
        replies.iter().all(|r| match &r.result {
            Ok(()) => true,
            Err(error) => {
                !matches!(error, StorageError::Cleanup { .. })
                    && matches!(
                        error.failure(),
                        StorageFailure::AlreadyExists
                            | StorageFailure::Conflict
                            | StorageFailure::RateLimited
                    )
            }
        }),
        "race writer returned an unexpected error or cleanup failure",
    )
}

/// All counted intervals share an instant. Throttled rounds still check every
/// known winner and the stored bytes before returning the longest retry delay.
/// A round refusing every writer passes only when `all_refused` allows it and
/// the stored bytes are `before`; the caller also compares the revision.
pub(crate) fn judge(
    replies: Vec<Reply>,
    payloads: &[Vec<u8>],
    stored: Option<&[u8]>,
    before: Option<&[u8]>,
    all_refused: AllRefused,
) -> Result<Round, StorageError> {
    replies_valid(&replies)?;
    let winner = replies.iter().find(|r| r.result.is_ok());
    require(
        stored == winner.map(|r| payloads[r.writer].as_slice()).or(before),
        "race changed or lost the confirmed bytes",
    )?;
    if replies.iter().any(|r| {
        r.result
            .as_ref()
            .is_err_and(|e| e.failure() == StorageFailure::RateLimited)
    }) {
        return Err(replies
            .into_iter()
            .filter_map(|r| r.result.err())
            .filter(|e| e.failure() == StorageFailure::RateLimited)
            .max_by_key(|e| throttle_wait(e, 0))
            .unwrap());
    }
    if winner.is_none() && before.is_some() && all_refused == AllRefused::Allowed {
        return Ok(Round::AllRefused);
    }
    let overlap = replies
        .iter()
        .map(|r| r.start)
        .max()
        .zip(replies.iter().map(|r| r.end).min())
        .is_some_and(|(start, end)| start < end);
    require(
        winner.is_some() && overlap,
        "race lacked one winner or overlapping requests",
    )?;
    Ok(Round::Won)
}

/// Ordinary backoff never shortens a provider's requested wait.
fn throttle_wait(error: &StorageError, attempt: u32) -> Duration {
    error
        .retry_after()
        .map_or(crate::retry_delay(attempt), |after| {
            after.wait().max(crate::retry_delay(attempt))
        })
}

/// A failed round must still respect writer cooldowns before cleanup.
pub(crate) async fn wait_for_writers(
    clock: &dyn Clock,
    replies: &[Reply],
) -> Result<(), StorageError> {
    if let Some(wait) = replies
        .iter()
        .filter_map(|r| r.result.as_ref().err())
        .filter(|e| e.failure() == StorageFailure::RateLimited)
        .map(|e| throttle_wait(e, 0))
        .max()
    {
        println!("sequence writers throttled wait_ms={}", wait.as_millis());
        clock.sleep(wait).await;
    }
    replies_valid(replies)
}

/// Retry auxiliary requests without repeating a publication whose
/// replies are already available for judging. Every wait is in the evidence.
pub(crate) async fn retry<T>(
    clock: &dyn Clock,
    mut request: impl AsyncFnMut() -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    for attempt in 0..20 {
        match request().await {
            Err(error)
                if error.failure() == StorageFailure::RateLimited
                    && !matches!(error, StorageError::Cleanup { .. }) =>
            {
                let wait = throttle_wait(&error, attempt);
                println!(
                    "sequence auxiliary throttled attempt={} wait_ms={}",
                    attempt + 1,
                    wait.as_millis()
                );
                clock.sleep(wait).await;
                if attempt == 19 {
                    return Err(error);
                }
            }
            result => return result,
        }
    }
    unreachable!()
}

/// An attempt gets its own slot name; a replacement fetches the current floor
/// again. Neither throttling nor a round refusing every writer advances the
/// count of completed rounds, which each have one winner.
pub(crate) async fn rounds(
    clock: &dyn Clock,
    label: &str,
    trials: u64,
    mut round: impl AsyncFnMut(u64, u64) -> Result<Round, StorageError>,
) -> Result<Tally, StorageError> {
    let mut completed = 0;
    let mut tally = Tally::default();
    let mut consecutive = 0;
    while completed < trials {
        // Pace independent rounds without separating the concurrent writers.
        clock.sleep(Duration::from_secs(1)).await;
        let attempt = completed + tally.throttled + tally.refused + 1;
        match round(completed + 1, attempt).await {
            Ok(Round::Won) => {
                completed += 1;
                consecutive = 0;
            }
            Ok(Round::AllRefused) => {
                tally.refused += 1;
                consecutive = 0;
                println!(
                    "sequence {label} attempt={attempt} completed={completed} all_refused={}",
                    tally.refused
                );
                if tally.refused >= 100 {
                    return Err(
                        StorageFailure::Protocol.with_source("100 rounds refused every writer")
                    );
                }
            }
            Err(error)
                if error.failure() == StorageFailure::RateLimited
                    && !matches!(error, StorageError::Cleanup { .. }) =>
            {
                tally.throttled += 1;
                let wait = throttle_wait(&error, consecutive);
                println!(
                    "sequence {label} attempt={attempt} completed={completed} throttled={} wait_ms={}",
                    tally.throttled,
                    wait.as_millis()
                );
                clock.sleep(wait).await;
                if tally.throttled >= 100 {
                    return Err(error);
                }
                consecutive += 1;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(tally)
}

pub(crate) async fn observed(
    clock: &dyn Clock,
    reader: &dyn Storage,
    path: &ObjectPath,
) -> Result<Option<crate::ObjectRead>, StorageError> {
    retry(clock, async || match reader.read_object(path, None).await {
        Ok(object) => Ok(Some(object)),
        Err(error) if error.failure() == StorageFailure::NotFound => Ok(None),
        Err(error) => Err(error),
    })
    .await
    .map_err(|error| {
        // A read never licenses rerunning an unchecked publication round.
        if error.failure() == StorageFailure::RateLimited {
            StorageFailure::Refused.with_source(error)
        } else {
            error
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
    clock: &dyn Clock,
    writer: &dyn Storage,
    reader: &dyn Storage,
    store: StoreId,
) -> Result<(), StorageError> {
    let slot = ObjectPath::slot(store, std::num::NonZeroU64::MIN);
    let bytes = b"slot whose successful create reply was withheld";
    let reply = lose_reply(retry(clock, async || writer.create(&slot, bytes).await))
        .await
        .unwrap_err();
    expect_failure(reply, StorageFailure::Network)?;
    require(
        retry(clock, async || reader.read(&slot).await).await? == bytes,
        "lost slot reply did not settle to the recorded bytes",
    )?;
    let retried = retry(clock, async || writer.create(&slot, bytes).await).await;
    match retried {
        Ok(_) => {}
        Err(error)
            if !matches!(error, StorageError::Cleanup { .. })
                && error.failure() == StorageFailure::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    require(
        retry(clock, async || reader.read(&slot).await).await? == bytes,
        "slot retry changed bytes",
    )?;
    let different = retry(clock, async || reader.create(&slot, b"another claim").await)
        .await
        .unwrap_err();
    expect_failure(different, StorageFailure::AlreadyExists)?;
    require(
        retry(clock, async || writer.read(&slot).await).await? == bytes,
        "refused slot retry changed bytes",
    )?;

    let floor = ObjectPath::floor(store);
    retry(clock, async || writer.create(&floor, b"first=1").await).await?;
    let previous = retry(clock, async || reader.read_object(&floor, None).await)
        .await?
        .metadata;
    let reply = lose_reply(retry(clock, async || {
        writer.replace(&previous, b"first=2").await
    }))
    .await
    .unwrap_err();
    expect_failure(reply, StorageFailure::Network)?;
    let confirmed = retry(clock, async || reader.read_object(&floor, None).await).await?;
    require(
        confirmed.bytes == b"first=2" && confirmed.metadata.revision != previous.revision,
        "lost floor reply did not settle to the new bytes and revision",
    )?;
    for bytes in [b"first=2", b"first=9"] {
        let retried = retry(clock, async || reader.replace(&previous, bytes).await)
            .await
            .unwrap_err();
        expect_failure(retried, StorageFailure::Conflict)?;
        let retained = retry(clock, async || writer.read_object(&floor, None).await).await?;
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
        let tally = rounds(clock, "create", trials, async |trial, attempt| {
            slot += 1;
            run(
                clock,
                &writers[..width],
                &ObjectPath::slot(store, slot.try_into().unwrap()),
                None,
                AllRefused::Fails,
                trial,
                attempt,
            )
            .await
        })
        .await?;
        println!("sequence SUMMARY create writers={width} trials={trials} overlapping={trials} winners={trials} throttled={} violations=0", tally.throttled);
    }
    Ok(())
}

/// Each stage counts `trials` one-winner rounds; `all_refused` says whether a
/// round refusing every writer is retried from a fresh observation.
pub(crate) async fn floor(
    clock: &dyn Clock,
    writers: &[Arc<dyn Storage>],
    store: StoreId,
    all_refused: AllRefused,
) -> Result<(), StorageError> {
    let path = ObjectPath::floor(store);
    for (width, trials) in STAGES {
        let tally = rounds(clock, "floor", trials, async |trial, attempt| {
            let previous = retry(clock, async || writers[0].read_object(&path, None).await).await?;
            for writer in &writers[1..width] {
                require(
                    retry(clock, async || writer.read_object(&path, None).await).await? == previous,
                    "replacers did not observe the same floor",
                )?;
            }
            run(
                clock,
                &writers[..width],
                &path,
                Some(&previous),
                all_refused,
                trial,
                attempt,
            )
            .await
        })
        .await?;
        println!("sequence SUMMARY floor writers={width} trials={trials} overlapping={trials} winners={trials} all_refused={} throttled={} violations=0", tally.refused, tally.throttled);
    }
    Ok(())
}

async fn run(
    clock: &dyn Clock,
    writers: &[Arc<dyn Storage>],
    path: &ObjectPath,
    previous: Option<&crate::ObjectRead>,
    all_refused: AllRefused,
    trial: u64,
    attempt: u64,
) -> Result<Round, StorageError> {
    let width = writers.len();
    let kind = if previous.is_some() {
        "floor"
    } else {
        "create"
    };
    let payloads: Vec<_> = (0..width)
        .map(|writer| {
            let mut bytes =
                format!("{kind} width={width} trial={trial} attempt={attempt} writer={writer} ")
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
                Some(previous) => storage.replace(&previous.metadata, bytes).await,
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
    for reply in &replies {
        println!("sequence {kind} writers={width} trial={trial} attempt={attempt} writer={} start_us={} end_us={} outcome={:?}",
            reply.writer, reply.start.as_micros(), reply.end.as_micros(), reply.result);
    }
    wait_for_writers(clock, &replies).await?;
    let stored = observed(clock, writers[0].as_ref(), path).await?;
    if let Some(previous) = previous {
        if replies.iter().any(|r| r.result.is_ok()) {
            require(
                stored
                    .as_ref()
                    .is_some_and(|s| s.metadata.revision != previous.metadata.revision),
                "floor winner retained the previous revision",
            )?;
        } else {
            require(
                stored.as_ref() == Some(previous),
                "refused floor writers changed its revision or bytes",
            )?;
        }
        require(
            replies.iter().all(|r| {
                !r.result
                    .as_ref()
                    .is_err_and(|e| e.failure() == StorageFailure::AlreadyExists)
            }),
            "floor writer returned a create conflict",
        )?;
    }
    let other = observed(clock, writers[width - 1].as_ref(), path).await?;
    require(
        other.as_ref().map(|s| &s.bytes) == stored.as_ref().map(|s| &s.bytes),
        "sequence race readers disagree on the winning bytes",
    )?;
    let round = judge(
        replies,
        &payloads,
        stored.as_ref().map(|s| s.bytes.as_slice()),
        previous.map(|p| p.bytes.as_slice()),
        all_refused,
    )?;
    if round == Round::Won && trial.is_multiple_of(25) {
        println!("sequence {kind} writers={width} completed={trial}");
    }
    Ok(round)
}

#[cfg(test)]
#[path = "sequence_live_tests.rs"]
mod tests;
