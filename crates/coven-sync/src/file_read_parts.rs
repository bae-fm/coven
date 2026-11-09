//! The parts an open file holds on a provider whose ranged read moves whole
//! parts, iCloud's: each part the file's requests need is fetched once, and
//! the ranges inside it are read from that fetch (§17.7, Appendix E.1).

use super::*;
use std::collections::{BTreeMap, VecDeque};

/// How many parts an open file keeps once its requests are done with them: a
/// read in order is in one, and a request that reached across a part's end
/// began the next (§17.7).
const KEPT: usize = 2;

/// The parts of one open file's object, which reads of any byte of a part
/// move whole.
pub(super) struct HeldParts {
    /// Bytes in each part, the last shorter.
    part: u64,
    /// The object's size.
    size: u64,
    /// The parts kept, the one used last at the back.
    kept: std::sync::Mutex<VecDeque<Arc<HeldPart>>>,
}

/// One part: its bytes once fetched, and the requests waiting for them.
struct HeldPart {
    index: u64,
    bytes: tokio::sync::OnceCell<Vec<u8>>,
    waiting: std::sync::Mutex<PartWaiters>,
}

/// The requests waiting for a part's fetch, each with the range of the
/// object it reads from the part, and how far through the object the fetch
/// has got.
struct PartWaiters {
    next: u64,
    requests: BTreeMap<u64, (ByteProgress, u64, u64)>,
    reached: u64,
}

impl HeldParts {
    /// An object of `size` bytes, read in parts of `part` bytes from its
    /// start.
    pub(super) fn new(part: std::num::NonZeroU64, size: u64) -> Self {
        Self { part: part.get(), size, kept: std::sync::Mutex::new(VecDeque::new()) }
    }

    /// The object's bytes `start..end`, from the parts holding them. A part
    /// not held is fetched whole by `fetch`, given its own range and what
    /// hears it arrive; a part another request is fetching is waited for.
    /// `received` hears how far through `start..end` the bytes reach.
    pub(super) async fn read<F, Fetched>(
        &self,
        start: u64,
        end: u64,
        received: &ByteProgress,
        fetch: F,
    ) -> Result<Vec<u8>, FileReadError>
    where
        F: Fn(u64, u64, ByteProgress) -> Fetched,
        Fetched: std::future::Future<Output = Result<Vec<u8>, FileReadError>>,
    {
        let mut bytes = Vec::with_capacity((end - start) as usize);
        let mut at = start;
        while at < end {
            // The part, kept as the one used last: the one held, or a new
            // one to fetch. Only the last `KEPT` stay kept; requests reading
            // one that goes still hold it.
            let part = {
                let index = at / self.part;
                let mut kept = coven_foundation::locks::lock_unpoisoned(&self.kept);
                let part = kept
                    .iter()
                    .position(|part| part.index == index)
                    .and_then(|position| kept.remove(position))
                    .unwrap_or_else(|| {
                        Arc::new(HeldPart {
                            index,
                            bytes: tokio::sync::OnceCell::new(),
                            waiting: std::sync::Mutex::new(PartWaiters {
                                next: 0,
                                requests: BTreeMap::new(),
                                reached: 0,
                            }),
                        })
                    });
                kept.push_back(part.clone());
                while kept.len() > KEPT {
                    kept.pop_front();
                }
                part
            };
            let from = part.index * self.part;
            let to = end.min(from + self.part);
            let held = part
                .read(at, to, received, || {
                    part.fetching();
                    let heard = part.clone();
                    fetch(
                        from,
                        (from + self.part).min(self.size),
                        ByteProgress::new(move |reached| heard.arrived(reached)),
                    )
                })
                .await?;
            let range = (at - from) as usize..(to - from) as usize;
            bytes.extend_from_slice(held.get(range).ok_or(FileReadError::Database(
                DbError::Invariant { what: "a held part covers the object's bytes in it" },
            ))?);
            at = to;
        }
        Ok(bytes)
    }

    /// Stop holding the parts `start..end` touches, so the next read of them
    /// fetches them again from storage.
    pub(super) fn forget(&self, start: u64, end: u64) {
        let (first, last) = (start / self.part, end.saturating_sub(1).max(start) / self.part);
        coven_foundation::locks::lock_unpoisoned(&self.kept)
            .retain(|part| part.index < first || part.index > last);
    }
}

impl HeldPart {
    /// This part's bytes, for a request reading `start..end` of the object
    /// from it, whom `received` tells how far through that range the part's
    /// fetch has got. A part not yet fetched is fetched by `fetch`, unless
    /// another request's fetch of it is under way; a fetch that fails or
    /// stops leaves it for the next request to fetch.
    async fn read<Fetched>(
        &self,
        start: u64,
        end: u64,
        received: &ByteProgress,
        fetch: impl FnOnce() -> Fetched,
    ) -> Result<&[u8], FileReadError>
    where
        Fetched: std::future::Future<Output = Result<Vec<u8>, FileReadError>>,
    {
        let id = {
            let mut waiting = coven_foundation::locks::lock_unpoisoned(&self.waiting);
            let id = waiting.next;
            waiting.next += 1;
            if waiting.reached > start {
                received.moved(waiting.reached.min(end));
            }
            waiting.requests.insert(id, (received.clone(), start, end));
            id
        };
        let bytes = self.bytes.get_or_try_init(fetch).await;
        coven_foundation::locks::lock_unpoisoned(&self.waiting).requests.remove(&id);
        let bytes = bytes?;
        received.moved(end);
        Ok(bytes)
    }

    /// A fetch of the part starts, after any that failed or stopped: no
    /// byte of it has arrived yet.
    fn fetching(&self) {
        coven_foundation::locks::lock_unpoisoned(&self.waiting).reached = 0;
    }

    /// The part's fetch has received the object's bytes up to `reached`:
    /// tell each waiting request how far through its own range that is.
    fn arrived(&self, reached: u64) {
        let mut waiting = coven_foundation::locks::lock_unpoisoned(&self.waiting);
        waiting.reached = waiting.reached.max(reached);
        for (received, start, end) in waiting.requests.values() {
            if reached > *start {
                received.moved(reached.min(*end));
            }
        }
    }
}
