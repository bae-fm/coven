#![cfg(test)]

//! Create-once races on one new CloudKit path. Every writer is its own adapter
//! instance and connection, though CloudKit returns one shared `CKContainer`
//! and `CKDatabase` for an identifier within a process, as the connect line
//! prints. All writers are released at one instant, and each round must end
//! with exactly one winner, every other writer refused as occupied, and the
//! winner's bytes stored.

use crate::providers::cloudkit::CloudKitStorage;
use crate::*;
use coven_foundation::{clock::Clock, id_source::IdSourceRef};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

/// Settled rounds of one race size, with the writers racing in each round.
#[derive(Clone, Copy, Debug)]
pub(super) struct Stage {
    pub(super) rounds: usize,
    pub(super) writers: usize,
}

pub(super) const STAGES: [Stage; 2] = [
    Stage {
        rounds: 200,
        writers: 2,
    },
    Stage {
        rounds: 50,
        writers: 8,
    },
];

/// A stage gives up after this many retried attempts in a row.
pub(super) const RETRIES: usize = 5;

/// The wait before retrying when no retryable refusal named one.
const RETRY_WAIT: Duration = Duration::from_secs(1);

/// Microseconds since the Unix epoch on this Mac's clock, which every process
/// of a race shares.
pub(super) fn now(clock: &dyn Clock) -> u64 {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_micros() as u64
}

/// The bytes `writer` tries to create in `attempt`, unique to both.
fn claim(writer: &str, attempt: u64) -> Vec<u8> {
    format!("{writer} attempt {attempt}").into_bytes()
}

/// One racing writer: its own adapter instance and connection.
pub(super) struct Writer {
    name: String,
    native: Arc<CloudKitStorage>,
    storage: Arc<StorageConnection<CloudKitStorage>>,
}

impl Writer {
    /// `count` writers named `<account>-<index>`, each from its own adapter.
    pub(super) async fn connect(
        account: &str,
        config: &StorageConfig,
        ids: &IdSourceRef,
        count: usize,
    ) -> Vec<Writer> {
        let mut writers = Vec::new();
        for index in 0..count {
            let native = Arc::new(
                CloudKitStorage::new(config.clone(), ids.clone())
                    .await
                    .unwrap(),
            );
            writers.push(Writer {
                name: format!("{account}-{index}"),
                storage: Arc::new(StorageConnection::from_provider(native.clone())),
                native,
            });
        }
        let containers: BTreeSet<_> = writers
            .iter()
            .map(|writer| objc2::rc::Retained::as_ptr(&writer.native.container) as usize)
            .collect();
        let databases: BTreeSet<_> = writers
            .iter()
            .map(|writer| objc2::rc::Retained::as_ptr(&writer.native.database) as usize)
            .collect();
        println!(
            "race {account} adapters={count} distinct CKContainer objects={} distinct CKDatabase objects={}",
            containers.len(),
            databases.len()
        );
        writers
    }
}

/// How one writer's create ended.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Verdict {
    Won,
    /// Refused as occupied, with the refusal and its native cause.
    Occupied(String),
    /// Rate limited or a network failure: the attempt says nothing about the race.
    Retryable {
        cause: String,
        retry_after_ms: Option<u64>,
    },
    /// Any other result, which no round may contain.
    Other(String),
}

impl Verdict {
    fn of(result: Result<Option<StoredObject>, StorageError>) -> Self {
        let error = match result {
            Ok(_) => return Self::Won,
            Err(error) => error,
        };
        let cause = error.to_string();
        if matches!(error, StorageError::Cleanup { .. }) {
            return Self::Other(cause);
        }
        match error.failure() {
            StorageFailure::AlreadyExists => Self::Occupied(cause),
            StorageFailure::RateLimited | StorageFailure::Network => Self::Retryable {
                retry_after_ms: error
                    .retry_after()
                    .map(|after| after.wait().as_millis() as u64),
                cause,
            },
            _ => Self::Other(cause),
        }
    }
}

/// One writer's result in one attempt, timed on the shared clock.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Outcome {
    writer: String,
    verdict: Verdict,
    /// How far past the release instant this writer's process learned of it.
    late_us: u64,
    started_us: u64,
    finished_us: u64,
}

/// Releases every writer at `release` (Unix microseconds) to create `path`.
/// After the one wait, every create is polled in the same pass, so each
/// writer's operation is enqueued before any reply can arrive.
pub(super) async fn race(
    clock: &dyn Clock,
    writers: &[Writer],
    path: &ObjectPath,
    attempt: u64,
    release: u64,
) -> Vec<Outcome> {
    let seen = now(clock);
    let late_us = seen.saturating_sub(release);
    clock
        .sleep(Duration::from_micros(release.saturating_sub(seen)))
        .await;
    futures_util::future::join_all(writers.iter().map(|writer| async move {
        let started_us = now(clock);
        let result = writer
            .storage
            .create(path, &claim(&writer.name, attempt))
            .await;
        Outcome {
            writer: writer.name.clone(),
            verdict: Verdict::of(result),
            late_us,
            started_us,
            finished_us: now(clock),
        }
    }))
    .await
}

/// What the owner does after an attempt.
pub(super) enum Settled {
    Round,
    /// Retry with a fresh path after this wait.
    Retry(Duration),
}

/// One stage's results.
#[derive(Default)]
pub(super) struct Tally {
    rounds: usize,
    attempts: usize,
    winners: BTreeMap<String, usize>,
    refusals: BTreeMap<String, usize>,
    retryable: BTreeMap<String, usize>,
    retried: Vec<String>,
    streak: usize,
    longest_streak: usize,
    /// Rounds in which every writer started before any finished.
    overlapping: usize,
    max_start_spread_us: u64,
    max_late_us: u64,
    anomalies: Vec<String>,
}

impl Tally {
    pub(super) fn rounds(&self) -> usize {
        self.rounds
    }

    pub(super) fn streak(&self) -> usize {
        self.streak
    }

    pub(super) fn anomalies(&self) -> &[String] {
        &self.anomalies
    }

    /// Records one attempt's outcomes and reads `path` back through `reader`.
    pub(super) async fn settle(
        &mut self,
        clock: &dyn Clock,
        reader: &StorageConnection<CloudKitStorage>,
        path: &ObjectPath,
        attempt: u64,
        outcomes: Vec<Outcome>,
    ) -> Settled {
        self.attempts += 1;
        for outcome in &outcomes {
            self.max_late_us = self.max_late_us.max(outcome.late_us);
        }
        let mut retry: Option<Duration> = None;
        for outcome in &outcomes {
            if let Verdict::Retryable {
                cause,
                retry_after_ms,
            } = &outcome.verdict
            {
                *self.retryable.entry(cause.clone()).or_default() += 1;
                let wait = retry_after_ms.map_or(RETRY_WAIT, Duration::from_millis);
                retry = Some(retry.map_or(wait, |longest| longest.max(wait)));
            }
        }
        let held = holder(clock, reader, path, attempt, &outcomes).await;
        if let Some(wait) = retry {
            self.streak += 1;
            self.longest_streak = self.longest_streak.max(self.streak);
            self.retried.push(format!(
                "attempt {attempt}: {outcomes:?}; path holds {held}"
            ));
            return Settled::Retry(wait);
        }
        self.streak = 0;
        self.rounds += 1;
        let winners: Vec<_> = outcomes
            .iter()
            .filter(|outcome| matches!(outcome.verdict, Verdict::Won))
            .collect();
        let mut anomalous = false;
        for outcome in &outcomes {
            match &outcome.verdict {
                Verdict::Won | Verdict::Retryable { .. } => {}
                Verdict::Occupied(cause) => *self.refusals.entry(cause.clone()).or_default() += 1,
                Verdict::Other(_) => anomalous = true,
            }
        }
        match winners.as_slice() {
            [winner] => {
                *self.winners.entry(winner.writer.clone()).or_default() += 1;
                if held != winner.writer {
                    anomalous = true;
                }
            }
            _ => anomalous = true,
        }
        if anomalous {
            self.anomalies.push(format!(
                "attempt {attempt}: {outcomes:?}; path holds {held}"
            ));
        }
        let latest_start = outcomes.iter().map(|o| o.started_us).max().unwrap();
        let earliest_start = outcomes.iter().map(|o| o.started_us).min().unwrap();
        let earliest_finish = outcomes.iter().map(|o| o.finished_us).min().unwrap();
        if latest_start < earliest_finish {
            self.overlapping += 1;
        }
        self.max_start_spread_us = self.max_start_spread_us.max(latest_start - earliest_start);
        Settled::Round
    }

    pub(super) fn print(&self, label: &str) {
        println!(
            "race {label}: rounds={} attempts={} retried={} longest retry streak={}",
            self.rounds,
            self.attempts,
            self.retried.len(),
            self.longest_streak
        );
        for (writer, count) in &self.winners {
            println!("race {label}: won by {writer}: {count}");
        }
        for (cause, count) in &self.refusals {
            println!("race {label}: refused {cause}: {count}");
        }
        for (cause, count) in &self.retryable {
            println!("race {label}: retryable {cause}: {count}");
        }
        for retried in &self.retried {
            println!("race {label}: retried {retried}");
        }
        println!(
            "race {label}: rounds where every writer started before any finished={}/{}; widest start spread={} us; latest release seen={} us late",
            self.overlapping, self.rounds, self.max_start_spread_us, self.max_late_us
        );
        println!("race {label}: anomalies={}", self.anomalies.len());
        for anomaly in &self.anomalies {
            println!("race {label}: anomaly {anomaly}");
        }
    }
}

/// Which writer's bytes `path` holds, "absent", or the read's failure.
async fn holder(
    clock: &dyn Clock,
    reader: &StorageConnection<CloudKitStorage>,
    path: &ObjectPath,
    attempt: u64,
    outcomes: &[Outcome],
) -> String {
    let mut tries = 0;
    loop {
        match reader.read(path).await {
            Ok(bytes) => {
                return outcomes
                    .iter()
                    .find(|outcome| claim(&outcome.writer, attempt) == bytes)
                    .map_or_else(
                        || format!("unrecognized bytes {:?}", String::from_utf8_lossy(&bytes)),
                        |outcome| outcome.writer.clone(),
                    )
            }
            Err(error) if error.failure() == StorageFailure::NotFound => return "absent".into(),
            Err(error)
                if matches!(
                    error.failure(),
                    StorageFailure::RateLimited | StorageFailure::Network
                ) && tries < RETRIES =>
            {
                tries += 1;
                clock
                    .sleep(error.retry_after().map_or(RETRY_WAIT, RetryAfter::wait))
                    .await;
            }
            Err(error) => return format!("unread: {error}"),
        }
    }
}

#[cfg(test)]
#[path = "cloudkit_race_tests.rs"]
mod tests;
