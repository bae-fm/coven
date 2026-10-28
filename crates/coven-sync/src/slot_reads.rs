//! Consecutive slots read in order, several requests ahead of the reader once
//! slots keep arriving (§21.1).

use crate::{object_input::ObjectInput, sequence_sync::SequenceSync, SyncError};
use futures_util::{stream::FuturesOrdered, StreamExt};
use std::{future::Future, pin::Pin};

/// The most slot reads in flight at once. The store's download limit also
/// bounds how many send at the same time (api.md E5).
const AHEAD: usize = 16;

/// A slot's bytes, and whether they came from the provider.
type SlotRead<'a> =
    Pin<Box<dyn Future<Output = Result<(Option<ObjectInput>, bool), SyncError>> + Send + 'a>>;

/// Reads slots `first`, `first + 1`, … up to `last`, returning them in order.
/// It starts with one read, so a quiet pass sends only the one miss, and
/// doubles the reads in flight with each slot the provider sends, up to
/// [`AHEAD`]; slots this device already holds don't widen it. So reading a
/// long history waits for one round trip per [`AHEAD`] slots rather than one
/// per slot, and past the end it sends at most `AHEAD - 1` reads whose slots
/// are absent. Dropping it cancels the reads still in flight; what they
/// received stays retained for the next read of the slot.
pub(crate) struct SlotReads<'a> {
    sync: &'a SequenceSync,
    /// The next slot to send a read for, None past `last`.
    requested: Option<u64>,
    last: u64,
    ahead: usize,
    reads: FuturesOrdered<SlotRead<'a>>,
}

impl<'a> SlotReads<'a> {
    pub(crate) fn new(sync: &'a SequenceSync, first: u64, last: u64) -> Self {
        Self {
            sync,
            requested: (first <= last).then_some(first),
            last,
            ahead: 1,
            reads: FuturesOrdered::new(),
        }
    }

    /// The next slot in order, None once it is absent or past `last`.
    pub(crate) async fn next(&mut self) -> Result<Option<ObjectInput>, SyncError> {
        while self.reads.len() < self.ahead {
            let Some(number) = self.requested else {
                break;
            };
            let sync = self.sync;
            self.reads.push_back(Box::pin(async move {
                let path = sync.path(number)?;
                match sync.held_slot(&path).await? {
                    Some(input) => Ok((Some(input), false)),
                    None => Ok((sync.read_slot(&path).await?, true)),
                }
            }));
            self.requested = number.checked_add(1).filter(|next| *next <= self.last);
        }
        let Some(read) = self.reads.next().await else {
            return Ok(None);
        };
        let (input, sent) = read?;
        if sent && input.is_some() {
            self.ahead = (self.ahead * 2).min(AHEAD);
        }
        Ok(input)
    }
}
