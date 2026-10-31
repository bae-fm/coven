use super::*;

pub(super) struct HeldRead {
    path: ObjectPath,
    observed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

/// A response body that stops at an object offset until released.
pub(super) struct HeldBody {
    path: ObjectPath,
    at: u64,
    observed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

/// Failures after response metadata has arrived.
#[derive(Clone, Copy)]
pub enum BodyFailure {
    /// EOF before the declared response length.
    Truncated(u64),
    /// A transport failure after this many response bytes, including at EOF.
    Interrupted(u64),
    /// A byte beyond the declared response length.
    ExtraByte,
}

impl MemoryStorage {
    /// Hold an exact read after fixing its response, including a missing object.
    pub async fn hold_next_read(
        &self,
        path: ObjectPath,
        observed: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_read.is_none(), "a read is already held");
        state.held_read = Some(HeldRead { path, observed, resume });
    }

    /// Stall the next response body for `path` that reaches object offset `at`,
    /// after delivering every byte before it, until `resume` is released.
    pub async fn hold_body_at(
        &self,
        path: ObjectPath,
        at: u64,
        observed: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_body.is_none(), "a body is already held");
        state.held_body = Some(HeldBody { path, at, observed, resume });
    }

    /// Buffers delivered by response bodies, without counting metadata as bytes.
    pub async fn received_buffers(&self) -> Vec<usize> {
        self.provider.state.lock().await.received_buffers.clone()
    }
}

struct MemoryBody {
    bytes: Arc<[u8]>,
    start: usize,
    end: usize,
    position: usize,
    chunk_size: usize,
    failure: Option<BodyFailure>,
    hold: Option<HeldBody>,
    state: Arc<Mutex<State>>,
}

#[async_trait]
impl ObjectBody for MemoryBody {
    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, StorageError> {
        let received = self.position - self.start;
        let end = match self.failure {
            Some(BodyFailure::Truncated(at) | BodyFailure::Interrupted(at)) => {
                if received as u64 >= at {
                    return match self.failure {
                        Some(BodyFailure::Truncated(_)) => Ok(0),
                        _ => Err(StorageFailure::Network.into()),
                    };
                }
                self.end.min(self.start.saturating_add(at as usize))
            }
            Some(BodyFailure::ExtraByte) if self.position == self.end => {
                output[0] = 1;
                self.failure = None;
                self.state.lock().await.received_buffers.push(1);
                return Ok(1);
            }
            _ => self.end,
        };
        let end = match &self.hold {
            Some(hold) if (hold.at as usize) > self.position => end.min(hold.at as usize),
            Some(_) => {
                let hold = self.hold.take().expect("held body");
                hold.observed
                    .send(())
                    .map_err(|_| StorageFailure::Protocol.with_source("body observer dropped"))?;
                hold.resume
                    .await
                    .map_err(|_| StorageFailure::Protocol.with_source("body release dropped"))?;
                end
            }
            None => end,
        };
        let length = (end - self.position).min(output.len()).min(self.chunk_size);
        output[..length].copy_from_slice(&self.bytes[self.position..self.position + length]);
        self.position += length;
        if length > 0 {
            self.state.lock().await.received_buffers.push(length);
        }
        Ok(length)
    }
}

impl MemoryProvider {
    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    pub(super) async fn stream_recorded(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'static>, StorageError> {
        self.before(MemoryRequest::Read(path.clone(), range)).await?;
        let mut state = self.state.lock().await;
        let result = (|| {
            if let Some(failure) = state.faults.read_failures.get(path) {
                return Err((*failure).into());
            }
            let object = state.objects.get(path).ok_or(StorageFailure::NotFound)?;
            let metadata = object.metadata(path, self.time_resolution);
            metadata.read_length(path, range)?;
            let start = range.map_or(0, ByteRange::start) as usize;
            let end = range.map_or(metadata.size, ByteRange::end) as usize;
            let body = MemoryBody {
                bytes: object.bytes.clone(),
                start,
                end,
                position: start,
                chunk_size: state.faults.read_chunk_size,
                failure: state.faults.body_failures.get(path).copied(),
                hold: None,
                state: self.state.clone(),
            };
            if body.chunk_size == 0 {
                return Err(StorageFailure::InvalidConfiguration.into());
            }
            let mut body = body;
            if state.held_body.as_ref().is_some_and(|held| {
                held.path == *path && (start as u64) < held.at && held.at < end as u64
            }) {
                body.hold = state.held_body.take();
            }
            if let Some(range) = range {
                state.ranges.push(range);
            }
            state.reads.push((path.clone(), start as u64, (end - start) as u64));
            ObjectStream::new(metadata, range, body)
        })();
        let held = if state.held_read.as_ref().is_some_and(|held| held.path == *path) {
            state.held_read.take()
        } else {
            None
        };
        drop(state);
        if let Some(held) = held {
            held.observed
                .send(())
                .map_err(|_| StorageFailure::Protocol.with_source("read observer dropped"))?;
            held.resume
                .await
                .map_err(|_| StorageFailure::Protocol.with_source("read release dropped"))?;
        }
        result
    }
}
