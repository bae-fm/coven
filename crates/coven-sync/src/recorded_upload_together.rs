//! A session's parts sent together, where the provider keeps each by its
//! index whatever order they arrive in (§17.9, Appendix E.10).

use super::{save_failure, UploadSource};
use coven_storage::{ByteProgress, ConfirmedPart, Storage, StorageError, UploadSession};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

type Reply = (u64, Result<ConfirmedPart, StorageError>);
type Sending<'a> = FuturesUnordered<Pin<Box<dyn Future<Output = Reply> + Send + 'a>>>;

/// How far the parts in flight have gone beside the confirmed bytes, for
/// whoever hears the upload: the confirmed bytes, and each part's bytes
/// handed over so far, by where it starts.
struct PartsMoving {
    hear: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    state: Mutex<(u64, BTreeMap<u64, u64>)>,
}

impl PartsMoving {
    fn tell(&self, state: &(u64, BTreeMap<u64, u64>)) {
        if let Some(hear) = &self.hear {
            hear(state.0 + state.1.values().sum::<u64>());
        }
    }
    /// The part starting at `start` ended, with `confirmed` bytes confirmed
    /// now: a confirmed part's bytes move from in flight to confirmed in one
    /// step, so what is heard never drops for it.
    fn ended(&self, start: u64, confirmed: u64) {
        let mut state = coven_foundation::locks::lock_unpoisoned(&self.state);
        state.1.remove(&start);
        state.0 = confirmed;
        self.tell(&state);
    }
    /// What hears the part starting at `start`, told how far through the
    /// object its bytes handed over reach.
    fn part(self: &Arc<Self>, start: u64) -> ByteProgress {
        if self.hear.is_none() {
            return ByteProgress::unread();
        }
        let moving = self.clone();
        ByteProgress::new(move |reached| {
            let mut state = coven_foundation::locks::lock_unpoisoned(&moving.state);
            state.1.insert(start, reached.saturating_sub(start));
            moving.tell(&state);
        })
    }
}

/// Run `work` while the parts in flight go on, keeping their replies.
async fn beside<T>(
    work: impl Future<Output = T>,
    sending: &mut Sending<'_>,
    replies: &mut Vec<Reply>,
) -> T {
    tokio::pin!(work);
    loop {
        tokio::select! {
            done = &mut work => return done,
            Some(reply) = sending.next(), if !sending.is_empty() => replies.push(reply),
        }
    }
}

/// Send the parts `session` still lacks, several at once: the first on the
/// attempt's own connection, each other on one the source can spare. Each
/// confirmed part is recorded as its reply comes in, whatever its order. A
/// failure, or a pause, starts no more parts; those in flight end first,
/// and those confirmed are recorded, so the next attempt resumes from every
/// part the provider holds.
pub(super) async fn send_together<S: UploadSource>(
    storage: &dyn Storage,
    session: &mut UploadSession,
    source: &mut S,
) -> Result<(), S::Error> {
    let moving = Arc::new(PartsMoving {
        hear: source.hears(),
        state: Mutex::new((session.confirmed_bytes(), BTreeMap::new())),
    });
    let mut sending: Sending<'_> = FuturesUnordered::new();
    let mut going = BTreeSet::new();
    // The connections taken beside the attempt's own: one fewer than the
    // parts in flight, whichever of them ends first.
    let mut spares = Vec::new();
    let mut replies: Vec<Reply> = Vec::new();
    let mut failed: Option<StorageError> = None;
    let mut unread: Option<S::Error> = None;
    loop {
        while let Some((start, reply)) = replies.pop() {
            going.remove(&start);
            spares.truncate(going.len().saturating_sub(1));
            let confirmed = reply.and_then(|part| session.confirm(part));
            moving.ended(start, session.confirmed_bytes());
            match confirmed {
                Ok(()) => {
                    let saved = beside(source.save(session.clone()), &mut sending, &mut replies);
                    if let Err(error) = saved.await {
                        unread.get_or_insert(error);
                    }
                }
                Err(error) => {
                    failed.get_or_insert(error);
                }
            }
        }
        let next = if failed.is_none() && unread.is_none() && source.keep_going() {
            session.parts_to_send().into_iter().find(|part| !going.contains(&part.start))
        } else {
            None
        };
        let connection = match &next {
            Some(_) if going.is_empty() => true,
            Some(_) => match source.spare_connection() {
                Some(spare) => {
                    spares.push(spare);
                    true
                }
                None => false,
            },
            None => false,
        };
        match next {
            Some(part) if connection => {
                let length = (part.end - part.start) as usize;
                let read = beside(source.read(part.start, length), &mut sending, &mut replies);
                let bytes = match read.await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        unread.get_or_insert(error);
                        spares.truncate(going.len().saturating_sub(1));
                        continue;
                    }
                };
                let sent = moving.part(part.start);
                let snapshot = session.clone();
                going.insert(part.start);
                sending.push(Box::pin(async move {
                    let reply =
                        storage.send_independent_part(&snapshot, part.start, &bytes, &sent).await;
                    (part.start, reply)
                }));
            }
            _ => match sending.next().await {
                Some(reply) => replies.push(reply),
                None => break,
            },
        }
    }
    if let Some(error) = unread {
        return Err(error);
    }
    match failed {
        Some(error) => Err(save_failure(source, session.clone(), error).await?.into()),
        None => Ok(()),
    }
}
