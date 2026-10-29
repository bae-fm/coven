//! Faults the in-memory provider injects into its next calls.

use super::BodyFailure;
use crate::*;
use std::collections::BTreeMap;
use std::time::Duration;

/// Faults applied to the next calls, including accepted parts with lost replies.
#[derive(Clone)]
pub struct Faults {
    /// Fail this many calls before doing their work.
    pub fail_next: usize,
    /// Fail exact reads at these paths, retaining other in-flight responses.
    pub read_failures: BTreeMap<ObjectPath, StorageFailure>,
    /// Fail exact status calls at these paths, independently of reads.
    pub status_failures: BTreeMap<ObjectPath, StorageFailure>,
    /// Fail exact deletions at these paths; the object stays.
    pub delete_failures: BTreeMap<ObjectPath, StorageFailure>,
    /// Refuse creates at these paths with a provider response of this
    /// HTTP status; nothing is stored.
    pub create_failures: BTreeMap<ObjectPath, (StorageFailure, u16)>,
    /// Fail listings of exactly these prefixes.
    pub list_failures: BTreeMap<ObjectPrefix, StorageFailure>,
    /// Maximum bytes returned by each body read; nonzero.
    pub read_chunk_size: usize,
    /// Failures after metadata, independently of request-start failures.
    pub body_failures: BTreeMap<ObjectPath, BodyFailure>,
    /// The classification of injected failures.
    pub failure: StorageFailure,
    /// Injected failures arrive as a provider response with this HTTP
    /// status (E5). None, without `retry_after`, is a request that received
    /// no response.
    pub response: Option<u16>,
    /// Injected failures arrive as a response, a 429 unless `response`
    /// names another status, asking every consumer to wait this many
    /// seconds (`Retry-After`).
    pub retry_after: Option<u64>,
    /// Delay each call without blocking its executor.
    pub delay: Duration,
    /// Hold each create, including injected failures, until all writers arrive.
    pub creation_barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
    /// Hold each create until the test adds a permit; each create takes
    /// one, oldest first, so a test releases held creates one at a time.
    pub creation_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Hold each part, its bytes taken, until the test adds a permit; each
    /// part takes one, oldest first.
    pub part_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Drop the next part before it is stored.
    pub drop_part: bool,
    /// Store the next part, but lose the reply before advancing the caller's session.
    pub lose_part_reply: bool,
    /// Publish the next upload but lose the completion reply.
    pub lose_completion_reply: bool,
    /// Refuse this many upload aborts, retaining the pending bytes.
    pub abort_failures: usize,
    /// Expire all pending sessions before the next call.
    pub expire_uploads: bool,
    /// Accept creates at occupied paths, violating the provider contract.
    pub overwrite_create: bool,
    /// Hide published objects from listings while exact reads and status still see them.
    pub hide_listings: bool,
    /// Hide objects no older than this lag; exact reads still see publication.
    pub listing_lag: Option<Duration>,
    /// Refuse a duplicate create but fail to abort its upload session.
    pub fail_duplicate_cleanup: bool,
    /// Store this many creates and replacements, then answer each with a
    /// 429, as a copy publication does when its temporary's deletion is
    /// refused after the object is published (§20.6).
    pub refuse_after_storing: usize,
    /// Refuse this many conditional replacements with `Conflict` although
    /// they name the current revision, leaving the object unchanged, as
    /// OneDrive can refuse every writer of one race.
    pub refuse_replacements: usize,
}
impl Faults {
    /// No failures or delays.
    pub fn none() -> Self {
        Self {
            fail_next: 0,
            read_failures: BTreeMap::new(),
            status_failures: BTreeMap::new(),
            delete_failures: BTreeMap::new(),
            create_failures: BTreeMap::new(),
            list_failures: BTreeMap::new(),
            read_chunk_size: 64 * 1024,
            body_failures: BTreeMap::new(),
            failure: StorageFailure::Network,
            response: None,
            retry_after: None,
            delay: Duration::ZERO,
            creation_barrier: None,
            creation_gate: None,
            part_gate: None,
            drop_part: false,
            lose_part_reply: false,
            lose_completion_reply: false,
            expire_uploads: false,
            abort_failures: 0,
            overwrite_create: false,
            fail_duplicate_cleanup: false,
            refuse_replacements: 0,
            refuse_after_storing: 0,
            hide_listings: false,
            listing_lag: None,
        }
    }
}
