//! What one connection has asked its provider: for each kind of request, how
//! many were sent, the body bytes they carried and the time spent waiting on
//! the provider. A caller reads the totals before and after a step and logs
//! the difference.

use coven_foundation::clock::ClockRef;
use std::{fmt, sync::Mutex, time::Duration};

/// A kind of provider request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    /// An exact metadata lookup.
    Status,
    /// A whole-object or ranged read, the bucket marker's included.
    Read,
    /// A single-request object creation, the bucket marker's included.
    Create,
    /// A conditional single-request floor replacement.
    Replace,
    /// One listing, across its pages.
    List,
    /// One deletion.
    Delete,
    /// A step of a recorded upload session: begin, part, resume, finish or abort.
    Upload,
    /// An account, sharing or joining request.
    Account,
}

impl RequestKind {
    const ALL: [Self; 8] = [
        Self::Status,
        Self::Read,
        Self::Create,
        Self::Replace,
        Self::List,
        Self::Delete,
        Self::Upload,
        Self::Account,
    ];

    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Read => "read",
            Self::Create => "create",
            Self::Replace => "replace",
            Self::List => "list",
            Self::Delete => "delete",
            Self::Upload => "upload",
            Self::Account => "account",
        }
    }
}

/// One kind's requests: how many were sent, the body bytes sent or received,
/// and the time from sending each until its response, plus the time spent
/// waiting for a read's body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestTotal {
    /// Requests sent, failed ones included.
    pub count: u64,
    /// Body bytes sent by writes or received by reads.
    pub bytes: u64,
    /// Time spent waiting on the provider.
    pub time: Duration,
}

/// Every kind's totals since the connection was made.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestCounts([RequestTotal; RequestKind::ALL.len()]);

impl RequestCounts {
    /// The totals of one kind.
    pub fn kind(&self, kind: RequestKind) -> RequestTotal {
        self.0[kind.index()]
    }

    /// Every kind's totals added together.
    pub fn total(&self) -> RequestTotal {
        self.0.iter().fold(RequestTotal::default(), |sum, kind| RequestTotal {
            count: sum.count + kind.count,
            bytes: sum.bytes + kind.bytes,
            time: sum.time + kind.time,
        })
    }

    /// The requests made after `earlier`, a reading of the same connection.
    pub fn since(&self, earlier: &Self) -> Self {
        let mut since = Self::default();
        for kind in RequestKind::ALL {
            let (now, then) = (self.kind(kind), earlier.kind(kind));
            since.0[kind.index()] = RequestTotal {
                count: now.count.saturating_sub(then.count),
                bytes: now.bytes.saturating_sub(then.bytes),
                time: now.time.saturating_sub(then.time),
            };
        }
        since
    }
}

/// Each kind that sent a request, as `read 41 (182044 B, 1.2s)`, or `none`.
impl fmt::Display for RequestCounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sent = RequestKind::ALL
            .into_iter()
            .filter(|kind| self.kind(*kind).count > 0 || self.kind(*kind).bytes > 0)
            .peekable();
        if sent.peek().is_none() {
            return f.write_str("none");
        }
        for (index, kind) in sent.enumerate() {
            let total = self.kind(kind);
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{} {} ({} B, {:?})", kind.name(), total.count, total.bytes, total.time)?;
        }
        Ok(())
    }
}

/// The running totals of one connection, timed by its clock.
pub(crate) struct RequestTally {
    clock: ClockRef,
    totals: Mutex<RequestCounts>,
}

impl RequestTally {
    pub(crate) fn new(clock: ClockRef) -> Self {
        Self { clock, totals: Mutex::new(RequestCounts::default()) }
    }

    /// The clock that times this connection's requests, and its waits.
    pub(crate) fn clock(&self) -> &ClockRef {
        &self.clock
    }

    pub(crate) fn counts(&self) -> RequestCounts {
        *self.totals.lock().expect("request tally poisoned")
    }

    /// Send one request of `kind` carrying `bytes`, counting it whether it
    /// succeeds or fails.
    pub(crate) async fn send<T>(
        &self,
        kind: RequestKind,
        bytes: u64,
        request: impl std::future::Future<Output = T>,
    ) -> T {
        let started = self.clock.elapsed();
        let result = request.await;
        self.add(kind, 1, bytes, self.clock.elapsed().saturating_sub(started));
        result
    }

    /// Wait for a read's body bytes, counting them and the wait as reading.
    pub(crate) async fn receive<T>(
        &self,
        read: impl std::future::Future<Output = Result<usize, T>>,
    ) -> Result<usize, T> {
        let started = self.clock.elapsed();
        let result = read.await;
        let received = result.as_ref().map_or(0, |length| *length as u64);
        self.add(RequestKind::Read, 0, received, self.clock.elapsed().saturating_sub(started));
        result
    }

    /// Count a read's bytes that arrived with its response.
    pub(crate) fn received(&self, bytes: u64) {
        self.add(RequestKind::Read, 0, bytes, Duration::ZERO);
    }

    fn add(&self, kind: RequestKind, count: u64, bytes: u64, time: Duration) {
        let mut totals = self.totals.lock().expect("request tally poisoned");
        let total = &mut totals.0[kind.index()];
        total.count += count;
        total.bytes += bytes;
        total.time += time;
    }
}

#[cfg(test)]
#[path = "request_tally_tests.rs"]
mod tests;
