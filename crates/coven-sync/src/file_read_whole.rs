//! A pin's whole copy of a stored file: several of its requests to storage
//! at once, as many as the download limit lets go, written to the copy in
//! order (§17.8).

use super::*;
use futures_util::StreamExt;

/// What a pin's download has in hand, for its meter: the bytes written to
/// the whole copy, and those its requests have received since, ahead of the
/// writing, in whatever order they arrive (§17.8).
pub(super) struct WholeProgress {
    meter: TransferMeter,
    /// Written, then received and not yet written.
    here: std::sync::Mutex<(u64, u64)>,
}

impl WholeProgress {
    fn new(meter: TransferMeter, written: u64) -> Self {
        meter.reached(written);
        Self { meter, here: std::sync::Mutex::new((written, 0)) }
    }
    /// A request received `bytes` more.
    pub(super) fn arrived(&self, bytes: u64) {
        let mut here = coven_foundation::locks::lock_unpoisoned(&self.here);
        here.1 += bytes;
        self.meter.reached(here.0 + here.1);
    }
    /// `bytes` more are written, `fetched` of them received before.
    fn wrote(&self, bytes: u64, fetched: u64) -> u64 {
        let mut here = coven_foundation::locks::lock_unpoisoned(&self.here);
        here.0 += bytes;
        here.1 -= fetched;
        self.meter.reached(here.0 + here.1);
        here.0
    }
}

impl UploadedFile {
    #[expect(
        clippy::map_err_ignore,
        reason = "a size whose encrypted layout overflows is the disagreement Integrity names"
    )]
    /// Assemble this file's whole encrypted cache copy, from cached and
    /// fetched chunks, check its content hash and sync it, under a reserved
    /// name: the copy a pin's write publishes (§17.8). Nothing is kept until
    /// then. `None` when a metered network the app keeps files off stops it
    /// before its next request to storage; what it had goes.
    pub(crate) async fn keep_whole(&self) -> Result<Option<CacheReservation>, TransferError> {
        // In flight until it ends: finished, failed or held, as what the
        // download came to says, or dropped with this call.
        let downloading = self.owner.database.downloads_in_flight().start(
            self.location.uploader(),
            self.location.object(),
            vec![self.file.id()],
            coven_database::TransferProgress {
                bytes_transferred: 0,
                bytes_total: self.header.encrypted_size().map_err(|_| self.integrity())?,
            },
        );
        let kept = self.assemble_whole(downloading.meter()).await;
        downloading.end(match &kept {
            Ok(Some(_)) => TransferEnd::Finished,
            Ok(None) => TransferEnd::Held,
            Err(_) => TransferEnd::Failed,
        });
        kept
    }

    /// Write the whole copy `keep_whole` keeps, telling `meter` how many of
    /// the encrypted object's bytes it has: cached chunks count as they are
    /// copied in, fetched ones as their bytes arrive. It continues the
    /// partial copy an earlier prepare left, once the bytes kept check. Up
    /// to the download limit's number of requests go at once, each taking
    /// its place under that limit as any download does, and their chunks
    /// are written in order: what is written is synced and kept after each
    /// request that follows on from it, so a prepare that stops loses only
    /// the requests under way and those that arrived ahead of one (§17.8).
    #[expect(
        clippy::map_err_ignore,
        reason = "the window is never closed, which the invariant the error names says"
    )]
    async fn assemble_whole(
        &self,
        meter: TransferMeter,
    ) -> Result<Option<CacheReservation>, TransferError> {
        let (mut reservation, mut writer, mut hash, next, written) = self.begin_whole().await?;
        let database = &self.owner.database;
        let progress = Arc::new(WholeProgress::new(meter, written));
        let at_once = self.owner.transfers.limits().downloads.get();
        let ranges = self.request_ranges(next)?;
        // A range holds its place in the window from its request until its
        // chunks are written and kept, so no more than the limit's number
        // are fetched or held at once. The requests go on while the chunks
        // before them are written, and the writing stops them all when it
        // ends early.
        let window = tokio::sync::Semaphore::new(at_once);
        let (sender, mut fetched) = tokio::sync::mpsc::channel(1);
        let fetch = async {
            let (window, progress) = (&window, &progress);
            let mut ranges = futures_util::stream::iter(ranges)
                .map(|(first, last)| async move {
                    let place = window.acquire().await.map_err(|_| {
                        FileReadError::Database(DbError::Invariant {
                            what: "a pin's download keeps its window open",
                        })
                    })?;
                    Ok::<_, FileReadError>((self.whole_range(first, last, progress).await?, place))
                })
                .buffered(at_once);
            while let Some(range) = ranges.next().await {
                if sender.send(range).await.is_err() {
                    break;
                }
            }
            drop(sender);
            std::future::pending::<std::convert::Infallible>().await
        };
        let write = async {
            let mut written = written;
            while let Some(range) = fetched.recv().await {
                // A metered network the app keeps files off stops the
                // download before its next request to storage (§17.8).
                let (range, _place) = range?;
                let Some((chunks, fetched)) = range else {
                    return Ok(None);
                };
                let mut bytes = 0;
                for chunk in chunks {
                    hash.update(&chunk.plaintext);
                    writer.append(&chunk.ciphertext).await?;
                    bytes += chunk.ciphertext.len() as u64;
                }
                writer.sync().await?;
                written = progress.wrote(bytes, fetched);
                database.keep_pin_download(&mut reservation, written).await?;
            }
            Ok::<_, TransferError>(Some(written))
        };
        let written = tokio::select! {
            written = write => written,
            never = fetch => match never {},
        };
        let Some(written) = written? else {
            return Ok(None);
        };
        if hash.finish() != self.hash {
            // Chunks that each authenticate but whole to another file: no
            // part of the copy is to be trusted again.
            drop(writer);
            drop(reservation);
            database.discard_pin_download(&self.file).await?;
            return Err(TransferError::Refused(crate::Refusal::ContentHash));
        }
        writer.finish().await?;
        database.keep_pin_download(&mut reservation, written).await?;
        Ok(Some(reservation))
    }

    /// The chunks from `first` to the file's last, split into what one
    /// request to storage fetches when none is cached: neighbouring chunks
    /// up to 1 MiB, a request from chunk zero counting the header it may
    /// carry (§17.7).
    #[expect(
        clippy::map_err_ignore,
        reason = "a chunk the file size's layout lacks is the disagreement Integrity names"
    )]
    fn request_ranges(&self, first: u64) -> Result<Vec<(u64, u64)>, FileReadError> {
        let count = self.header.chunk_count();
        let mut ranges = Vec::new();
        let mut start = first;
        while start < count {
            let from = match start {
                0 => 0,
                _ => self.header.chunk(start).map_err(|_| self.integrity())?.offset,
            };
            let mut last = start;
            while last + 1 < count
                && self.header.chunk(last + 1).map_err(|_| self.integrity())?.end() - from
                    <= REQUEST_BYTES
            {
                last += 1;
            }
            ranges.push((start, last));
            start = last + 1;
        }
        Ok(ranges)
    }

    /// Chunks `first..=last` for the whole copy, cached or fetched, and how
    /// many of their bytes were fetched; `None` when a metered network holds
    /// the download.
    async fn whole_range(
        &self,
        first: u64,
        last: u64,
        progress: &Arc<WholeProgress>,
    ) -> Result<Option<(Vec<OpenedChunk>, u64)>, FileReadError> {
        let fetched = Arc::new(AtomicU64::new(0));
        let mut chunks = Vec::new();
        let mut index = first;
        while index <= last {
            let destination = ChunkDestination::WholeFile { progress, fetched: &fetched };
            let ChunkStep::Read(end, read) = self.chunks(index, last, destination).await? else {
                return Ok(None);
            };
            chunks.extend(read);
            index = end + 1;
        }
        Ok(Some((chunks, fetched.load(Ordering::Relaxed))))
    }
}
