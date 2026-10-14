use super::*;

pub(super) struct HeldRead {
    path: ObjectPath,
    observed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
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
        state.held_read = Some(HeldRead {
            path,
            observed,
            resume,
        });
    }
}

impl MemoryProvider {
    pub(super) async fn read_recorded(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        self.before(MemoryRequest::Read(path.clone(), range))
            .await?;
        let mut state = self.state.lock().await;
        let result = (|| {
            if let Some(failure) = state.faults.read_failures.get(path) {
                return Err((*failure).into());
            }
            let object = state.objects.get(path).ok_or(StorageFailure::NotFound)?;
            let metadata = object.metadata(path, self.time_resolution);
            let bytes = match range {
                Some(range) => range.select(&object.bytes)?,
                None => object.bytes.clone(),
            };
            if let Some(range) = range {
                state.ranges.push(range);
            }
            state.reads.push((
                path.clone(),
                range.map_or(0, ByteRange::start),
                bytes.len() as u64,
            ));
            Ok(ObjectRead { metadata, bytes })
        })();
        let held = if state
            .held_read
            .as_ref()
            .is_some_and(|held| held.path == *path)
        {
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
