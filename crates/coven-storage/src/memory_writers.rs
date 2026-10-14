use super::*;
use coven_foundation::id_source::{DeviceId, StoreId};

impl MemoryProvider {
    pub(super) async fn writer_folders(
        &self,
        store: StoreId,
    ) -> Result<Vec<DeviceId>, StorageError> {
        self.before(MemoryRequest::Other("list_writers")).await?;
        let prefix = ObjectPrefix::store_logs(store);
        let now = self.clock.now();
        let mut state = self.state.lock().await;
        let writers: std::collections::BTreeSet<_> = state
            .objects
            .iter()
            .filter(|(path, object)| {
                !state.faults.hide_listings
                    && prefix.contains(path)
                    && state.faults.listing_lag.is_none_or(|lag| {
                        now.duration_since(object.stored_at)
                            .is_ok_and(|age| age > lag)
                    })
            })
            .filter_map(|(path, _)| path.store_log_position().map(|(writer, _)| writer))
            .collect();
        let held = if state
            .held_listing
            .as_ref()
            .is_some_and(|held| held.prefix == prefix)
        {
            state.held_listing.take()
        } else {
            None
        };
        drop(state);
        if let Some(held) = held {
            held.listed
                .send(())
                .map_err(|_| StorageFailure::Protocol.with_source("listing observer dropped"))?;
            held.resume
                .await
                .map_err(|_| StorageFailure::Protocol.with_source("listing release dropped"))?;
        }
        Ok(writers.into_iter().collect())
    }
}
