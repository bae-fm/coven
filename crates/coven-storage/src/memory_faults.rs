//! Faults the in-memory provider injects into its next calls.

use super::BodyFailure;
use crate::*;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// Faults applied to the next calls, including accepted parts with lost replies.
#[derive(Clone)]
pub struct Faults {
    /// Fail this many calls before doing their work.
    pub fail_next: usize,
    /// Refuse this many file uploads as they start, each with `failure`,
    /// `response` and `retry_after` as `fail_next` fails a call: a file's
    /// single create, or the call beginning its multipart upload. Nothing is
    /// stored. Every other call goes through, so a pass beside the upload
    /// can't take the fault.
    pub refuse_file_uploads: usize,
    /// Panic in this many file uploads as they start, as a bug in the
    /// caller's upload would; every other call goes through.
    pub panic_file_uploads: usize,
    /// Panic in exact reads at these paths, as a bug in the reader would.
    pub read_panics: BTreeSet<ObjectPath>,
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
    /// Change one byte of responses at these paths, each response
    /// differently, as faults on the way to the device would; the stored
    /// object stays as it was.
    pub garbled_reads: BTreeMap<ObjectPath, Garble>,
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
    /// Drop the next part before it is stored.
    pub drop_part: bool,
    /// Store the next part, but lose the reply before advancing the caller's session.
    pub lose_part_reply: bool,
    /// Start the next planned session's upload, but lose the reply: the
    /// caller never learns the id of the session the provider holds.
    pub lose_initiation_reply: bool,
    /// Publish the next upload but lose the completion reply.
    pub lose_completion_reply: bool,
    /// Store the next create or replacement at each of these paths, then
    /// lose its reply, once per path.
    pub lose_reply_at: BTreeSet<ObjectPath>,
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
    /// Fail every grant of access to an account with this failure, as a
    /// provider refusing to share the store would.
    pub grant_failure: Option<StorageFailure>,
    /// Fail this many acceptances of a join's invite before they change
    /// anything, as a provider that cannot be reached would.
    pub accept_failures: usize,
    /// Fail this many undos of a join's acceptance before they change
    /// anything, as a provider that cannot be reached would.
    pub leave_failures: usize,
}
/// Faults on the way to the device that change one byte of a response.
#[derive(Clone, Copy, Debug)]
pub struct Garble {
    /// The object offset whose byte changes, in a response that holds it.
    pub at: u64,
    /// How many such responses change.
    pub reads: u8,
}

impl Faults {
    /// No failures or delays.
    pub fn none() -> Self {
        Self {
            fail_next: 0,
            refuse_file_uploads: 0,
            panic_file_uploads: 0,
            read_panics: BTreeSet::new(),
            read_failures: BTreeMap::new(),
            status_failures: BTreeMap::new(),
            delete_failures: BTreeMap::new(),
            create_failures: BTreeMap::new(),
            list_failures: BTreeMap::new(),
            read_chunk_size: 64 * 1024,
            body_failures: BTreeMap::new(),
            garbled_reads: BTreeMap::new(),
            failure: StorageFailure::Network,
            response: None,
            retry_after: None,
            delay: Duration::ZERO,
            drop_part: false,
            lose_part_reply: false,
            lose_initiation_reply: false,
            lose_completion_reply: false,
            lose_reply_at: BTreeSet::new(),
            expire_uploads: false,
            abort_failures: 0,
            overwrite_create: false,
            fail_duplicate_cleanup: false,
            refuse_replacements: 0,
            refuse_after_storing: 0,
            grant_failure: None,
            accept_failures: 0,
            leave_failures: 0,
            hide_listings: false,
            listing_lag: None,
        }
    }
}
